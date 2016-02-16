// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;
using System.Globalization;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Task
{
    internal sealed class TaskStatus
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TaskStatus));

        private readonly object _stateLock = new object();
        private readonly TaskLifeCycle _taskLifeCycle;
        private readonly IHeartBeatManager _heartBeatManager;
        private readonly Optional<ISet<ITaskMessageSource>> _evaluatorMessageSources;
        private readonly string _taskId;
        private readonly string _contextId;

        private Optional<Exception> _lastException = Optional<Exception>.Empty();
        private Optional<byte[]> _result = Optional<byte[]>.Empty();
        private TaskState _state;

        [Inject]
        private TaskStatus(
            [Parameter(typeof(ContextConfigurationOptions.ContextIdentifier))] string contextId,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(TaskConfigurationOptions.TaskMessageSources))] ISet<ITaskMessageSource> taskMessageSources,
            TaskLifeCycle taskLifeCycle,
            IHeartBeatManager heartBeatManager)
        {
            _heartBeatManager = heartBeatManager;
            _taskLifeCycle = taskLifeCycle;
            _evaluatorMessageSources = Optional<ISet<ITaskMessageSource>>.OfNullable(taskMessageSources);
            State = TaskState.Init;
            _taskId = taskId;
            _contextId = contextId;
        }

        /// <summary>
        /// TODO[JIRA REEF-1167]: Remove constructor.
        /// </summary>
        [Obsolete("Deprecated in 0.14. Will be removed.")]
        public TaskStatus(IHeartBeatManager heartBeatManager, string contextId, string taskId, Optional<ISet<ITaskMessageSource>> evaluatorMessageSources)
        {
            _heartBeatManager = heartBeatManager;
            _taskLifeCycle = TangFactory.GetTang().NewInjector().GetInstance<TaskLifeCycle>();
            _evaluatorMessageSources = evaluatorMessageSources;
            State = TaskState.Init;
            _taskId = taskId;
            _contextId = contextId;
        }

        public TaskState State
        {
            get
            {
                return _state;
            }

            private set
            {
                if (IsLegalStateTransition(_state, value))
                {
                    _state = value;
                }
                else
                {
                    string message = string.Format(CultureInfo.InvariantCulture, "Illegal state transition from [{0}] to [{1}]", _state, value);
                    LOGGER.Log(Level.Error, message);
                    Utilities.Diagnostics.Exceptions.Throw(new InvalidOperationException(message), LOGGER);
                }
            }
        }

        public string TaskId
        {
            get { return _taskId; }
        }

        public string ContextId
        {
            get { return _contextId; }
        }

        public void SetException(Exception e)
        {
            lock (_stateLock)
            {
                if (!_lastException.IsPresent())
                {
                    _lastException = Optional<Exception>.Of(e);
                }
                State = TaskState.Failed;
                _taskLifeCycle.Stop();
                Heartbeat();
            }
        }

        public void SetResult(byte[] result)
        {
            lock (_stateLock)
            {
                _result = Optional<byte[]>.OfNullable(result);
                switch (State)
                {
                    case TaskState.SuspendRequested:
                        State = TaskState.Suspended;
                        break;
                    case TaskState.Running:
                    case TaskState.CloseRequested:
                        State = TaskState.Done;
                        break;
                }
                _taskLifeCycle.Stop();
                Heartbeat();
            }
        }

        public void SetRunning()
        {
            lock (_stateLock)
            {
                LOGGER.Log(Level.Verbose, "TaskStatus::SetRunning");
                if (_state == TaskState.Init)
                {
                    try
                    {
                        _taskLifeCycle.Start();
                        
                        // Need to send an INIT heartbeat to the driver prompting it to create an RunningTask event. 
                        LOGGER.Log(Level.Info, "Sending task INIT heartbeat");
                        Heartbeat();
                        State = TaskState.Running;
                    }
                    catch (Exception e)
                    {
                        Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Cannot set task status to running.", LOGGER);
                        SetException(e);
                    }
                }
            }
        }

        public void SetCloseRequested()
        {
            lock (_stateLock)
            {
                if (HasEnded())
                {
                    return;
                }

                State = TaskState.CloseRequested;
            }
        }

        public void SetSuspendRequested()
        {
            lock (_stateLock)
            {
                if (HasEnded())
                {
                    return;
                }

                State = TaskState.SuspendRequested;
            }
        }

        public void SetKilled()
        {
            lock (_stateLock)
            {
                if (HasEnded())
                {
                    LOGGER.Log(Level.Warning, "Trying to kill a task that is in {0} state. Ignored.", State);
                    return;
                }

                State = TaskState.Killed;
                Heartbeat();
            }
        }

        public bool IsNotRunning()
        {
            return _state != TaskState.Running;
        }

        public bool HasEnded()
        {
            switch (_state)
            {
                case TaskState.Done:
                case TaskState.Suspended:
                case TaskState.Failed:
                case TaskState.Killed:
                    return true;
                default:
                    return false;
            }
        }

        public TaskStatusProto ToProto()
        {
            // This is locked because the Task continuation thread which sets the
            // result is potentially different from the HeartBeat thread.
            lock (_stateLock)
            {
                Check();
                TaskStatusProto taskStatusProto = new TaskStatusProto()
                {
                    context_id = ContextId,
                    task_id = TaskId,
                    state = GetProtoState()
                };
                if (_result.IsPresent())
                {
                    taskStatusProto.result = ByteUtilities.CopyBytesFrom(_result.Value);
                }
                else if (_lastException.IsPresent())
                {
                    byte[] error = ByteUtilities.StringToByteArrays(_lastException.Value.ToString());
                    taskStatusProto.result = ByteUtilities.CopyBytesFrom(error);
                }
                else if (_state == TaskState.Running)
                {
                    foreach (TaskMessage message in GetMessages())
                    {
                        TaskStatusProto.TaskMessageProto taskMessageProto = new TaskStatusProto.TaskMessageProto()
                        {
                            source_id = message.MessageSourceId,
                            message = ByteUtilities.CopyBytesFrom(message.Message),
                        };
                        taskStatusProto.task_message.Add(taskMessageProto);
                    }
                }
                return taskStatusProto;
            }
        }

        private static bool IsLegalStateTransition(TaskState? from, TaskState to)
        {
            if (from == null)
            {
                return to == TaskState.Init;
            }

            if (from == to)
            {
                LOGGER.Log(Level.Warning, "Transitioning to the same state from {0} to {1}.", from, to);
                return true;
            }

            switch (from)
            {
                case TaskState.Init:
                    switch (to)
                    {
                        case TaskState.Init:
                        case TaskState.Running:
                        case TaskState.Failed:
                        case TaskState.Killed:
                        case TaskState.Done:
                            return true;
                        default:
                            return false;
                    }
                case TaskState.Running:
                    switch (to)
                    {
                        case TaskState.CloseRequested:
                        case TaskState.SuspendRequested:
                        case TaskState.Failed:
                        case TaskState.Killed:
                        case TaskState.Done:
                            return true;
                        default:
                            return false;
                    }
                case TaskState.CloseRequested:
                    switch (to)
                    {
                        case TaskState.Failed:
                        case TaskState.Killed:
                        case TaskState.Done:
                            return true;
                        default:
                            return false;
                    }
                case TaskState.SuspendRequested:
                    switch (to)
                    {
                        case TaskState.Failed:
                        case TaskState.Killed:
                        case TaskState.Suspended:
                            return true;
                        default:
                            return false;
                    }

                case TaskState.Failed:
                case TaskState.Done:
                case TaskState.Killed:
                    return false;
                default:
                    LOGGER.Log(Level.Error, "Unknown \"from\" state: {0}", from);
                    return false;
            }
        }

        private void Check()
        {
            if (_result.IsPresent() && _lastException.IsPresent())
            {
                LOGGER.Log(Level.Warning, "Both task result and exception are present, the expcetion will take over. Thrown away result:" + ByteUtilities.ByteArraysToString(_result.Value));
                State = TaskState.Failed;
                _result = Optional<byte[]>.Empty();
            }
        }

        private void Heartbeat()
        {
            _heartBeatManager.OnNext(ToProto());
        }

        private State GetProtoState()
        {
            switch (_state)
            {
                case TaskState.Init:
                    return Protobuf.ReefProtocol.State.INIT;
                case TaskState.CloseRequested:
                case TaskState.SuspendRequested:
                case TaskState.Running:
                    return Protobuf.ReefProtocol.State.RUNNING;
                case TaskState.Done:
                    return Protobuf.ReefProtocol.State.DONE;
                case TaskState.Suspended:
                    return Protobuf.ReefProtocol.State.SUSPEND;
                case TaskState.Failed:
                    return Protobuf.ReefProtocol.State.FAILED;
                case TaskState.Killed:
                    return Protobuf.ReefProtocol.State.KILLED;
                default:
                    Utilities.Diagnostics.Exceptions.Throw(new InvalidOperationException("Unknown state: " + _state), LOGGER);
                    break;
            }
            return Protobuf.ReefProtocol.State.FAILED; // this line should not be reached as default case will throw exception
        }

        private ICollection<TaskMessage> GetMessages()
        {
            List<TaskMessage> result = new List<TaskMessage>();
            if (_evaluatorMessageSources.IsPresent())
            {
                foreach (ITaskMessageSource source in _evaluatorMessageSources.Value)
                {
                    Optional<TaskMessage> taskMessageOptional = source.Message;
                    if (taskMessageOptional.IsPresent())
                    {
                        result.Add(taskMessageOptional.Value);
                    }
                }
            }
            return result;
        }
    }
}
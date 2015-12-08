/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Task
{
    internal sealed class TaskStatus
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TaskStatus));
        private readonly TaskLifeCycle _taskLifeCycle;
        private readonly HeartBeatManager _heartBeatManager;
        private readonly Optional<ISet<ITaskMessageSource>> _evaluatorMessageSources;

        private readonly string _taskId;
        private readonly string _contextId;
        private Optional<Exception> _lastException = Optional<Exception>.Empty();
        private Optional<byte[]> _result = Optional<byte[]>.Empty();
        private TaskState _state;

        public TaskStatus(HeartBeatManager heartBeatManager, string contextId, string taskId, Optional<ISet<ITaskMessageSource>> evaluatorMessageSources)
        {
            _contextId = contextId;
            _taskId = taskId;
            _heartBeatManager = heartBeatManager;
            _taskLifeCycle = new TaskLifeCycle();
            _evaluatorMessageSources = evaluatorMessageSources;
            State = TaskState.Init;
        }

        public TaskState State
        {
            get
            {
                return _state;
            }

            set
            {
                if (IsLegalStateTransition(_state, value))
                {
                    _state = value;
                }
                else
                {
                    string message = string.Format(CultureInfo.InvariantCulture, "Illegal state transition from [{0}] to [{1}]", _state, value);
                    LOGGER.Log(Level.Error, message);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new InvalidOperationException(message), LOGGER);
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
            RecordExecptionWithoutHeartbeat(e);
            Heartbeat();
            _lastException = Optional<Exception>.Empty();
        }

        public void SetResult(byte[] result)
        {
            _result = Optional<byte[]>.OfNullable(result);
            if (State == TaskState.Running)
            {
                State = TaskState.Done;
            }
            else if (State == TaskState.SuspendRequested)
            {
                State = TaskState.Suspended;
            }
            else if (State == TaskState.CloseRequested)
            {
                State = TaskState.Done;
            }
            _taskLifeCycle.Stop();
            Heartbeat();
        }

        public void SetRunning()
        {
            LOGGER.Log(Level.Verbose, "TaskStatus::SetRunning");
            if (_state == TaskState.Init)
            {
                try
                {
                    _taskLifeCycle.Start();

                    // Need to send an INIT heartbeat to the driver prompting it to create an RunningTask event. 
                    LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Sending task INIT heartbeat"));
                    Heartbeat();
                    State = TaskState.Running;
                }
                catch (Exception e)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Cannot set task status to running.", LOGGER);
                    SetException(e);
                }
            }
        }

        public void SetCloseRequested()
        {
            State = TaskState.CloseRequested;
        }

        public void SetSuspendRequested()
        {
            State = TaskState.SuspendRequested;
        }

        public void SetKilled()
        {
            State = TaskState.Killed;
            Heartbeat();
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
            Check();
            TaskStatusProto taskStatusProto = new TaskStatusProto()
            {
                context_id = _contextId,
                task_id = _taskId,
                state = GetProtoState(),
            };
            if (_result.IsPresent())
            {
                taskStatusProto.result = ByteUtilities.CopyBytesFrom(_result.Value);
            }
            else if (_lastException.IsPresent())
            {
                // final Encoder<Throwable> codec = new ObjectSerializableCodec<>();
                // final byte[] error = codec.encode(_lastException.get());
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

        internal void RecordExecptionWithoutHeartbeat(Exception e)
        {
            if (!_lastException.IsPresent())
            {
                _lastException = Optional<Exception>.Of(e);
            }
            State = TaskState.Failed;
            _taskLifeCycle.Stop();
        }

        private static bool IsLegalStateTransition(TaskState? from, TaskState to)
        {
            if (from == null)
            {
                return to == TaskState.Init;
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
                default:
                    return true;
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
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new InvalidOperationException("Unknown state: " + _state), LOGGER);
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
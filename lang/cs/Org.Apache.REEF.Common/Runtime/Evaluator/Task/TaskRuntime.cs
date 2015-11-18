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
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Task
{
    public class TaskRuntime : IObserver<ICloseEvent>, IObserver<ISuspendEvent>, IObserver<IDriverMessage>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TaskRuntime));
        
        private readonly ITask _task;

        private readonly IInjector _injector;

        // The memento given by the task configuration
        private readonly Optional<byte[]> _memento;

        private readonly HeartBeatManager _heartBeatManager;

        private readonly TaskStatus _currentStatus;

        private readonly INameClient _nameClient;

        private readonly Lazy<IDriverConnectionMessageHandler> _driverConnectionMessageHandler;

        public TaskRuntime(IInjector taskInjector, string contextId, string taskId, HeartBeatManager heartBeatManager, string memento = null)
        {
            _injector = taskInjector;
            _heartBeatManager = heartBeatManager;

            Optional<ISet<ITaskMessageSource>> messageSources = Optional<ISet<ITaskMessageSource>>.Empty();
            try
            {
                _task = _injector.GetInstance<ITask>();
            }
            catch (Exception e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.CaughtAndThrow(new InvalidOperationException("Unable to inject task.", e), Level.Error, "Unable to inject task.", LOGGER);
            }
            try
            {
                ITaskMessageSource taskMessageSource = _injector.GetInstance<ITaskMessageSource>();
                messageSources = Optional<ISet<ITaskMessageSource>>.Of(new HashSet<ITaskMessageSource>() { taskMessageSource });
            }
            catch (Exception e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, "Cannot inject task message source with error: " + e.StackTrace, LOGGER);
                // do not rethrow since this is benign
            }
            try
            {
                _nameClient = _injector.GetInstance<INameClient>();
                _heartBeatManager.EvaluatorSettings.NameClient = _nameClient;
            }
            catch (InjectionException)
            {
                LOGGER.Log(Level.Warning, "Cannot inject name client from task configuration.");
                // do not rethrow since user is not required to provide name client
            }

            _driverConnectionMessageHandler = new Lazy<IDriverConnectionMessageHandler>(() =>
            {
                try
                {
                    return _injector.GetInstance<IDriverConnectionMessageHandler>();
                }
                catch (InjectionException)
                {
                    LOGGER.Log(Level.Info, "User did not implement IDriverConnectionMessageHandler.");
                }

                return null;
            });

            LOGGER.Log(Level.Info, "task message source injected");
            _currentStatus = new TaskStatus(_heartBeatManager, contextId, taskId, messageSources);
            _memento = memento == null ?
                Optional<byte[]>.Empty() : Optional<byte[]>.Of(ByteUtilities.StringToByteArrays(memento));
        }

        public string TaskId
        {
            get { return _currentStatus.TaskId; }
        }

        public string ContextId
        {
            get { return _currentStatus.ContextId; }
        }

        public void Initialize()
        {
            _currentStatus.SetRunning();
        }

        /// <summary>
        /// Run the task
        /// </summary>
        public void Start()
        {
            try
            {
                LOGGER.Log(Level.Info, "Call Task");
                if (_currentStatus.IsNotRunning())
                {
                    var e = new InvalidOperationException("TaskRuntime not in Running state, instead it is in state " + _currentStatus.State);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }

                byte[] taskMemento = _memento.IsPresent() ? _memento.Value : null;
                var result = RunTask(taskMemento);

                LOGGER.Log(Level.Info, "Task Call Finished");
                _currentStatus.SetResult(result);
                if (result != null && result.Length > 0)
                {
                    LOGGER.Log(Level.Info, "Task running result:\r\n" + System.Text.Encoding.Default.GetString(result));
                }
            }
            catch (Exception e)
            {
                LOGGER.Log(Level.Warning,
                    string.Format(CultureInfo.InvariantCulture, "Task failed caused by exception [{0}]", e));
                _currentStatus.SetException(e);
            }
            finally
            {
                _task.Dispose();
            }
        }

        public TaskState GetTaskState()
        {
            return _currentStatus.State;
        }

        /// <summary>
        /// Called by heartbeat manager
        /// </summary>
        /// <returns>  current TaskStatusProto </returns>
        public TaskStatusProto GetStatusProto()
        {
            return _currentStatus.ToProto();
        }

        public bool HasEnded()
        {
            return _currentStatus.HasEnded();
        }

        /// <summary>
        /// get ID of the task.
        /// </summary>
        /// <returns>ID of the task.</returns>
        public string GetActicityId()
        {
            return _currentStatus.TaskId;
        }

        public void Close(byte[] message)
        {
            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Trying to close Task {0}", TaskId));
            if (_currentStatus.IsNotRunning())
            {
                LOGGER.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Trying to close an task that is in {0} state. Ignored.", _currentStatus.State));
            }
            else
            {
                try
                {
                    OnNext(new CloseEventImpl(message));
                    _currentStatus.SetCloseRequested();
                }
                catch (Exception e)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Error during Close.", LOGGER);

                    _currentStatus.SetException(
                        new TaskClientCodeException(TaskId, ContextId, "Error during Close().", e));
                }
            }
        }

        public void Suspend(byte[] message)
        {
            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Trying to suspend Task {0}", TaskId));
            
            if (_currentStatus.IsNotRunning())
            {
                LOGGER.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Trying to supend an task that is in {0} state. Ignored.", _currentStatus.State));
            }
            else
            {
                try
                {
                    OnNext(new SuspendEventImpl(message));
                    _currentStatus.SetSuspendRequested();
                }
                catch (Exception e)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Error during Suspend.", LOGGER);
                    _currentStatus.SetException(
                        new TaskClientCodeException(TaskId, ContextId, "Error during Suspend().", e));
                }
            }
        }

        public void Deliver(byte[] message)
        {
            if (_currentStatus.IsNotRunning())
            {
                LOGGER.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Trying to send a message to an task that is in {0} state. Ignored.", _currentStatus.State));
            }
            else
            {
                try
                {
                    OnNext(new DriverMessageImpl(message));
                }
                catch (Exception e)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Error during message delivery.", LOGGER);
                    _currentStatus.SetException(
                        new TaskClientCodeException(TaskId, ContextId, "Error during message delivery.", e));
                }
            }
        }

        public void OnNext(ICloseEvent value)
        {
            LOGGER.Log(Level.Info, "TaskRuntime::OnNext(ICloseEvent value)");
            // TODO: send a heartbeat
        }

        void IObserver<ICloseEvent>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<IDriverMessage>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        void IObserver<IDriverMessage>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<ISuspendEvent>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        void IObserver<ISuspendEvent>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<ICloseEvent>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnNext(ISuspendEvent value)
        {
            LOGGER.Log(Level.Info, "TaskRuntime::OnNext(ISuspendEvent value)");
            // TODO: send a heartbeat
        }

        public void OnNext(IDriverMessage value)
        {
            IDriverMessageHandler messageHandler = null;
            LOGGER.Log(Level.Info, "TaskRuntime::OnNext(IDriverMessage value)");
            try
            {
                messageHandler = _injector.GetInstance<IDriverMessageHandler>();
            }
            catch (Exception e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.CaughtAndThrow(e, Level.Error, "Received Driver message, but unable to inject handler for driver message ", LOGGER);
            }
            if (messageHandler != null)
            {
                try
                {
                    messageHandler.Handle(value);
                }
                catch (Exception e)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, "Exception throw when handling driver message: " + e, LOGGER);
                    _currentStatus.RecordExecptionWithoutHeartbeat(e);
                }
            }
        }

        /// <summary>
        /// Propagates the IDriverConnection message to the Handler as specified by the Task.
        /// </summary>
        internal void HandleDriverConnectionMessage(IDriverConnectionMessage message)
        {
            if (_driverConnectionMessageHandler.Value == null)
            {
                return;
            }

            _driverConnectionMessageHandler.Value.OnNext(message);
        }

        private byte[] RunTask(byte[] memento)
        {
            return _task.Call(memento);
        }
    }
}
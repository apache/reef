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
using System.Threading;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Common.Tasks.Exceptions;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Task
{
    internal sealed class TaskRuntime : IObserver<ICloseEvent>, IObserver<ISuspendEvent>, IObserver<IDriverMessage>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskRuntime));

        private readonly TaskStatus _currentStatus;
        private readonly Optional<IDriverConnectionMessageHandler> _driverConnectionMessageHandler;
        private readonly Optional<IDriverMessageHandler> _driverMessageHandler;
        private readonly ITask _userTask;
        private readonly IInjectionFuture<IObserver<ISuspendEvent>> _suspendHandlerFuture;
        private int _taskRan = 0;

        [Inject]
        private TaskRuntime(
            ITask userTask,
            IDriverMessageHandler driverMessageHandler, 
            IDriverConnectionMessageHandler driverConnectionMessageHandler,
            TaskStatus taskStatus,
            [Parameter(typeof(TaskConfigurationOptions.SuspendHandler))] IInjectionFuture<IObserver<ISuspendEvent>> suspendHandlerFuture)
        {
            _currentStatus = taskStatus;
            _driverMessageHandler = Optional<IDriverMessageHandler>.Of(driverMessageHandler);
            _driverConnectionMessageHandler = Optional<IDriverConnectionMessageHandler>.Of(driverConnectionMessageHandler);
            _userTask = userTask;
            _suspendHandlerFuture = suspendHandlerFuture;
        }

        /// <summary>
        /// TODO[JIRA REEF-1167]: Remove constructor.
        /// </summary>
        [Obsolete("Deprecated in 0.14. Will be removed.")]
        public TaskRuntime(IInjector taskInjector, string contextId, string taskId, IHeartBeatManager heartBeatManager)
        {
            var messageSources = Optional<ISet<ITaskMessageSource>>.Empty();
            try
            {
                var taskMessageSource = taskInjector.GetInstance<ITaskMessageSource>();
                messageSources = Optional<ISet<ITaskMessageSource>>.Of(new HashSet<ITaskMessageSource> { taskMessageSource });
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, "Cannot inject task message source with error: " + e.StackTrace, Logger);

                // do not rethrow since this is benign
            }

            try
            {
                _driverConnectionMessageHandler = Optional<IDriverConnectionMessageHandler>.Of(taskInjector.GetInstance<IDriverConnectionMessageHandler>());
            }
            catch (InjectionException)
            {
                Logger.Log(Level.Info, "User did not implement IDriverConnectionMessageHandler.");
                _driverConnectionMessageHandler = Optional<IDriverConnectionMessageHandler>.Empty();
            }

            try
            {
                _driverMessageHandler = Optional<IDriverMessageHandler>.Of(taskInjector.GetInstance<IDriverMessageHandler>());
            }
            catch (InjectionException)
            {
                Logger.Log(Level.Info, "User did not implement IDriverMessageHandler.");
                _driverMessageHandler = Optional<IDriverMessageHandler>.Empty();
            }

            try
            {
                _userTask = taskInjector.GetInstance<ITask>();
            }
            catch (InjectionException ie)
            {
                const string errorMessage = "User did not implement IDriverMessageHandler.";
                Utilities.Diagnostics.Exceptions.CaughtAndThrow(ie, Level.Error, errorMessage, Logger);
            }

            Logger.Log(Level.Info, "task message source injected");
            _currentStatus = new TaskStatus(heartBeatManager, contextId, taskId, messageSources);
        }

        public string TaskId
        {
            get { return _currentStatus.TaskId; }
        }

        public string ContextId
        {
            get { return _currentStatus.ContextId; }
        }

        /// <summary>
        /// Runs the task asynchronously.
        /// </summary>
        public void RunTask()
        {
            if (Interlocked.Exchange(ref _taskRan, 1) != 0)
            {
                // Return if we have already called RunTask
                throw new InvalidOperationException("TaskRun has already been called on TaskRuntime.");
            }

            // Send heartbeat such that user receives a TaskRunning message.
            _currentStatus.SetRunning();
            
            System.Threading.Tasks.Task.Run(() =>
            {
                Logger.Log(Level.Info, "Calling into user's task.");
                return _userTask.Call(null);
            }).ContinueWith(runTask =>
                {
                    try
                    {
                        // Task failed.
                        if (runTask.IsFaulted)
                        {
                            Logger.Log(Level.Warning,
                                string.Format(CultureInfo.InvariantCulture, "Task failed caused by exception [{0}]", runTask.Exception));
                            _currentStatus.SetException(runTask.Exception);
                            return;
                        }

                        if (runTask.IsCanceled)
                        {
                            Logger.Log(Level.Warning,
                                string.Format(CultureInfo.InvariantCulture, "Task failed caused by task cancellation"));
                            return;
                        }

                        // Task completed.
                        var result = runTask.Result;
                        Logger.Log(Level.Info, "Task Call Finished");
                        _currentStatus.SetResult(result);
                        if (result != null && result.Length > 0)
                        {
                            Logger.Log(Level.Info, "Task running result:\r\n" + System.Text.Encoding.Default.GetString(result));
                        }
                    }
                    finally
                    {
                        if (_userTask != null)
                        {
                            _userTask.Dispose();
                        }

                        runTask.Dispose();
                    }
                });
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

        public void Close(byte[] message)
        {
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Trying to close Task {0}", TaskId));
            if (_currentStatus.IsNotRunning())
            {
                Logger.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Trying to close an task that is in {0} state. Ignored.", _currentStatus.State));
                return;
            }
            try
            {
                OnNext(new CloseEventImpl(message));
                _currentStatus.SetCloseRequested();
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Error during Close.", Logger);
                _currentStatus.SetException(new TaskClientCodeException(TaskId, ContextId, "Error during Close().", e));
            }
        }

        public void Suspend(byte[] message)
        {
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Trying to suspend Task {0}", TaskId));

            if (_currentStatus.IsNotRunning())
            {
                Logger.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Trying to supend an task that is in {0} state. Ignored.", _currentStatus.State));
                return;
            }
            try
            {
                OnNext(new SuspendEventImpl(message));
                _currentStatus.SetSuspendRequested();
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Error during Suspend.", Logger);
                _currentStatus.SetException(
                    new TaskClientCodeException(TaskId, ContextId, "Error during Suspend().", e));
            }
        }

        public void Deliver(byte[] message)
        {
            if (_currentStatus.IsNotRunning())
            {
                Logger.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Trying to send a message to an task that is in {0} state. Ignored.", _currentStatus.State));
                return;
            }
            try
            {
                OnNext(new DriverMessageImpl(message));
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Error during message delivery.", Logger);
                _currentStatus.SetException(
                    new TaskClientCodeException(TaskId, ContextId, "Error during message delivery.", e));
            }
        }

        public void OnNext(ICloseEvent value)
        {
            Logger.Log(Level.Info, "TaskRuntime::OnNext(ICloseEvent value)");
            //// TODO: send a heartbeat
        }

        public void OnNext(ISuspendEvent value)
        {
            Logger.Log(Level.Info, "TaskRuntime::OnNext(ISuspendEvent value)");
            try
            {
                _suspendHandlerFuture.Get().OnNext(value);
            }
            catch (Exception ex)
            {
                var suspendEx = new TaskSuspendHandlerException("Unable to suspend task.", ex);
                Utilities.Diagnostics.Exceptions.CaughtAndThrow(suspendEx, Level.Error, Logger);
            }
        }

        public void OnNext(IDriverMessage value)
        {
            Logger.Log(Level.Info, "TaskRuntime::OnNext(IDriverMessage value)");

            if (!_driverMessageHandler.IsPresent())
            {
                return;
            }
            try
            {
                _driverMessageHandler.Value.Handle(value);
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, "Exception throw when handling driver message: " + e, Logger);
                _currentStatus.SetException(e);
            }
        }

        /// <summary>
        /// Propagates the IDriverConnection message to the Handler as specified by the Task.
        /// </summary>
        internal void HandleDriverConnectionMessage(IDriverConnectionMessage message)
        {
            if (!_driverConnectionMessageHandler.IsPresent())
            {
                return;
            }

            _driverConnectionMessageHandler.Value.OnNext(message);
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}
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
using System.Globalization;
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Common.Tasks.Exceptions;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;
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
        private readonly IInjectionFuture<IObserver<ICloseEvent>> _closeHandlerFuture;
        private int _taskRan = 0;

        [Inject]
        private TaskRuntime(
            ITask userTask,
            IDriverMessageHandler driverMessageHandler, 
            IDriverConnectionMessageHandler driverConnectionMessageHandler,
            TaskStatus taskStatus,
            [Parameter(typeof(TaskConfigurationOptions.SuspendHandler))] IInjectionFuture<IObserver<ISuspendEvent>> suspendHandlerFuture,
            [Parameter(typeof(TaskConfigurationOptions.CloseHandler))] IInjectionFuture<IObserver<ICloseEvent>> closedHandlerFuture)
        {
            _currentStatus = taskStatus;
            _driverMessageHandler = Optional<IDriverMessageHandler>.Of(driverMessageHandler);
            _driverConnectionMessageHandler = Optional<IDriverConnectionMessageHandler>.Of(driverConnectionMessageHandler);
            _userTask = userTask;
            _suspendHandlerFuture = suspendHandlerFuture;
            _closeHandlerFuture = closedHandlerFuture;
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
        /// For testing only!
        /// </summary>
        [Testing]
        internal ITask Task 
        { 
            get { return _userTask; }
        }

        /// <summary>
        /// Runs the task asynchronously.
        /// </summary>
        public System.Threading.Tasks.Task RunTask()
        {
            if (Interlocked.Exchange(ref _taskRan, 1) != 0)
            {
                // Return if we have already called RunTask
                throw new InvalidOperationException("TaskRun has already been called on TaskRuntime.");
            }

            // Send heartbeat such that user receives a TaskRunning message.
            _currentStatus.SetRunning();

            return System.Threading.Tasks.Task.Run(() =>
            {
                Logger.Log(Level.Info, "Calling into user's task.");
                return _userTask.Call(null);
            }).ContinueWith((System.Threading.Tasks.Task<byte[]> runTask) =>
                {
                    try
                    {
                        // Task failed.
                        if (runTask.IsFaulted)
                        {
                            OnTaskFailure(runTask);
                            return;
                        }

                        if (runTask.IsCanceled)
                        {
                            Logger.Log(Level.Error,
                                string.Format(CultureInfo.InvariantCulture, "Task failed caused by System.Threading.Task cancellation"));
                            OnTaskFailure(runTask);
                            return;
                        }

                        // Task completed.
                        var result = runTask.Result;
                        Logger.Log(Level.Info, "Task Call Finished");
                        _currentStatus.SetResult(result);

                        const Level resultLogLevel = Level.Verbose;

                        if (Logger.CustomLevel >= resultLogLevel && result != null && result.Length > 0)
                        {
                            Logger.Log(resultLogLevel,
                                "Task running result:\r\n" + System.Text.Encoding.Default.GetString(result));
                        }
                    }
                    finally
                    {
                        try
                        {
                            if (_userTask != null)
                            {
                                _userTask.Dispose();
                            }

                            runTask.Dispose();
                        }
                        catch (Exception e)
                        {
                            Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Error when disposing Task.", Logger);
                        }
                    }
                });
        }

        /// <summary>
        /// Sets the current status of the Task with the Exception it failed with.
        /// </summary>
        private void OnTaskFailure(System.Threading.Tasks.Task runTask)
        {
            if (runTask.Exception == null)
            {
                _currentStatus.SetException(new SystemException("Task failed without an Exception."));
            }
            else
            {
                var aggregateException = runTask.Exception.Flatten();
                _currentStatus.SetException(
                    aggregateException.InnerExceptions.Count == 1 ?
                    aggregateException.InnerExceptions.First() : aggregateException);
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
                Logger.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Trying to suspend an task that is in {0} state. Ignored.", _currentStatus.State));
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
            _closeHandlerFuture.Get().OnNext(value);

            // TODO: send a heartbeat
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
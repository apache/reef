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

using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.Threading;

namespace Org.Apache.REEF.Bridge.Core.Tests.Fail.Driver
{
    internal sealed class NoopTask :
        ITask,
        ITaskMessageSource,
        IDriverMessageHandler,
        IObserver<ISuspendEvent>,
        IObserver<ITaskStop>,
        IObserver<ICloseEvent>
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(NoopTask));

        private static readonly TaskMessage InitMessage =
            TaskMessage.From("nooptask", ByteUtilities.StringToByteArrays("MESSAGE::INIT"));

        private bool _isRunning = true;
        private Optional<TaskMessage> _message = Utilities.Optional<TaskMessage>.Empty();

        [Inject]
        private NoopTask()
        {
            Log.Log(Level.Info, "NoopTask created.");
        }

        public byte[] Call(byte[] memento)
        {
            _isRunning = true;
            while (_isRunning)
            {
                try
                {
                    Log.Log(Level.Info, "NoopTask.call(): Waiting for the message.");
                    lock (this)
                    {
                        Monitor.Wait(this);
                    }
                }
                catch (System.Threading.ThreadInterruptedException ex)
                {
                    Log.Log(Level.Warning, "NoopTask.wait() interrupted.", ex);
                }
            }

            Log.Log(Level.Info,
                "NoopTask.call(): Exiting with message {0}",
                ByteUtilities.ByteArraysToString(_message.OrElse(InitMessage).Message));
            return _message.OrElse(InitMessage).Message;
        }

        public Optional<TaskMessage> Message
        {
            get
            {
                Log.Log(Level.Info,
                    "NoopTask.getMessage() invoked: {0}",
                    ByteUtilities.ByteArraysToString(_message.OrElse(InitMessage).Message));
                return _message;
            }
        }

        public void Dispose()
        {
            Log.Log(Level.Info, "NoopTask.stopTask() invoked.");
            _isRunning = false;
            lock (this)
            {
                Monitor.Pulse(this);
            }
        }

        public void OnError(System.Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Handler for SuspendEvent.
        /// </summary>
        /// <param name="suspendEvent"></param>
        public void OnNext(ISuspendEvent suspendEvent)
        {
            Log.Log(Level.Info, "NoopTask.TaskSuspendHandler.OnNext() invoked.");
            Dispose();
        }

        /// <summary>
        /// Handler for TaskStop.
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(ITaskStop value)
        {
            Log.Log(Level.Info, "NoopTask.TaskStopHandler.OnNext() invoked.");
        }

        /// <summary>
        /// Handler for CloseEvent.
        /// </summary>
        /// <param name="closeEvent"></param>
        public void OnNext(ICloseEvent closeEvent)
        {
            Log.Log(Level.Info, "NoopTask.TaskCloseHandler.OnNext() invoked.");
            Dispose();
        }

        /// <summary>
        /// Handler for DriverMessage.
        /// </summary>
        /// <param name="driverMessage"></param>
        public void Handle(IDriverMessage driverMessage)
        {
            byte[] msg = driverMessage.Message.Value;
            Log.Log(Level.Info,
                "NoopTask.DriverMessageHandler.Handle() invoked: {0}",
                ByteUtilities.ByteArraysToString(msg));
            lock (this)
            {
                _message = Utilities.Optional<TaskMessage>.Of(TaskMessage.From("nooptask", msg));
            }
        }
    }
}
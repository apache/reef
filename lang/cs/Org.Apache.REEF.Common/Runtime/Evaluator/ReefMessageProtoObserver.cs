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
using System.Globalization;
using System.Threading;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Common.Runtime.Evaluator
{
    internal sealed class ReefMessageProtoObserver :
        IObserver<IRemoteMessage<REEFMessage>>,
        IObservable<IRemoteMessage<REEFMessage>>,
        IDisposable
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ReefMessageProtoObserver));
        private volatile IObserver<IRemoteMessage<REEFMessage>> _observer = null;
        private long _count = 0;
        private DateTime _begin;
        private DateTime _origBegin;

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(IRemoteMessage<REEFMessage> value)
        {
            REEFMessage remoteEvent = value.Message;
            IRemoteIdentifier id = value.Identifier;
            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "receive a ReefMessage from {0} Driver at {1}.", remoteEvent, id));

            if (remoteEvent.evaluatorControl != null)
            {
                if (remoteEvent.evaluatorControl.context_control != null)
                {
                    string context_message = null;
                    string task_message = null;

                    if (remoteEvent.evaluatorControl.context_control.context_message != null)
                    {
                        context_message = remoteEvent.evaluatorControl.context_control.context_message.ToString();
                    }
                    if (remoteEvent.evaluatorControl.context_control.task_message != null)
                    {
                        task_message = ByteUtilities.ByteArraysToString(remoteEvent.evaluatorControl.context_control.task_message);
                    }

                    if (!(string.IsNullOrEmpty(context_message) && string.IsNullOrEmpty(task_message)))
                    {
                        LOGGER.Log(Level.Info, 
                            string.Format(CultureInfo.InvariantCulture, "Control protobuf with context message [{0}] and task message [{1}]", context_message, task_message));
                    }                   
                    else if (remoteEvent.evaluatorControl.context_control.remove_context != null)
                    {
                         LOGGER.Log(Level.Info, 
                            string.Format(CultureInfo.InvariantCulture, "Control protobuf to remove context {0}", remoteEvent.evaluatorControl.context_control.remove_context.context_id));
                    }
                    else if (remoteEvent.evaluatorControl.context_control.add_context != null)
                    {
                        LOGGER.Log(Level.Info, 
                            string.Format(CultureInfo.InvariantCulture, "Control protobuf to add a context on top of {0}", remoteEvent.evaluatorControl.context_control.add_context.parent_context_id));
                    }
                    else if (remoteEvent.evaluatorControl.context_control.start_task != null)
                    {
                        LOGGER.Log(Level.Info, 
                            string.Format(CultureInfo.InvariantCulture, "Control protobuf to start an task in {0}", remoteEvent.evaluatorControl.context_control.start_task.context_id));
                    }
                    else if (remoteEvent.evaluatorControl.context_control.stop_task != null)
                    {
                        LOGGER.Log(Level.Info, "Control protobuf to stop task");
                    }
                    else if (remoteEvent.evaluatorControl.context_control.suspend_task != null)
                    {
                        LOGGER.Log(Level.Info, "Control protobuf to suspend task"); 
                    }
                }
            } 
            if (_count == 0)
            {
                _begin = DateTime.Now;
                _origBegin = _begin;
            }
            var count = Interlocked.Increment(ref _count);

            int printBatchSize = 100000;
            if (count % printBatchSize == 0)
            {
                DateTime end = DateTime.Now;
                var diff = (end - _begin).TotalMilliseconds;
                double seconds = diff / 1000.0;
                long eventsPerSecond = (long)(printBatchSize / seconds);
                _begin = DateTime.Now;
            }

            var observer = _observer;
            if (observer != null)
            {
                observer.OnNext(value);
            }
        }

        public IDisposable Subscribe(IObserver<IRemoteMessage<REEFMessage>> observer)
        {
            if (_observer != null)
            {
                return null;
            }
            _observer = observer;
            return this;
        }

        public void Dispose()
        {
            _observer = null;
        }
    }
}

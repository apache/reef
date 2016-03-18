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
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Common.Tasks.Exceptions;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Tasks.Defaults
{
    /// <summary>
    /// A default handler for an event from the driver signaling the suspension
    /// of a task. Throws an exception by default, since a task should not have received
    /// a suspension event if the handler is not bound explicitly.
    /// </summary>
    internal sealed class DefaultTaskCloseHandler : IObserver<ICloseEvent>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DefaultTaskCloseHandler));

        [Inject]
        private DefaultTaskCloseHandler()
        {
        }

        public void OnCompleted()
        {
            Utilities.Diagnostics.Exceptions.Throw(new NotImplementedException(), Logger);
        }

        public void OnError(Exception error)
        {
            Utilities.Diagnostics.Exceptions.Throw(new NotImplementedException(), Logger);
        }

        public void OnNext(ICloseEvent value)
        {
            Utilities.Diagnostics.Exceptions.Throw(new TaskCloseHandlerNotBoundException("No EventHandler<CloseEvent> registered. Event received: " + value), Logger);
        }
    }
}
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
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Common.Tasks.Defaults
{
    /// <summary>
    /// A default handler for an event from the driver signaling the suspension
    /// of a task. Throws an exception by default, since a task should not have received
    /// a suspension event if the handler is not bound explicitly.
    /// </summary>
    internal sealed class DefaultSuspendHandler : IObserver<ISuspendEvent>
    {
        [Inject]
        private DefaultSuspendHandler()
        {
        }

        public void OnNext(ISuspendEvent value)
        {
            throw new Exception("No handler for suspend registered.");
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
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
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Evaluator.Tests.TestUtils
{
    internal sealed class TestTask : ITask, IObserver<ITaskStop>
    {
        [Inject]
        private TestTask()
        {
            CountDownEvent = new CountdownEvent(1);
            StopEvent = new CountdownEvent(1);
            DisposedEvent = new CountdownEvent(1);
        }

        public CountdownEvent CountDownEvent { get; private set; }

        public CountdownEvent StopEvent { get; private set; }

        public CountdownEvent DisposedEvent { get; private set; }

        public void Dispose()
        {
            DisposedEvent.Signal();
        }

        public byte[] Call(byte[] memento)
        {
            CountDownEvent.Wait();
            return null;
        }

        public void OnNext(ITaskStop value)
        {
            StopEvent.Signal();
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
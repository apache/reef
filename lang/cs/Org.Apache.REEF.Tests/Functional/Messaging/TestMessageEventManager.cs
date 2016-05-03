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

using System.Threading;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Tests.Functional.Messaging
{
    /// <summary>
    /// A test helper class that keeps track of whether context/task messages 
    /// have been sent.
    /// </summary>
    public sealed class TestMessageEventManager
    {
        private readonly ManualResetEventSlim _isContextMessageSentEvent = new ManualResetEventSlim(false);
        private readonly ManualResetEventSlim _isTaskMessageSentEvent = new ManualResetEventSlim(false);

        [Inject]
        private TestMessageEventManager()
        {
        }

        /// <summary>
        /// Returns true if context message has been sent.
        /// </summary>
        public WaitHandle IsContextMessageSentEvent
        {
            get { return _isContextMessageSentEvent.WaitHandle; }
        }

        /// <summary>
        /// Returns true if task message has been sent.
        /// </summary>
        public WaitHandle IsTaskMessageSentEvent
        {
            get { return _isTaskMessageSentEvent.WaitHandle; }
        }

        /// <summary>
        /// Called when Task message is sent.
        /// </summary>
        public void OnTaskMessageSent()
        {
            _isTaskMessageSentEvent.Set();
        }

        /// <summary>
        /// Called when Context message is sent.
        /// </summary>
        public void OnContextMessageSent()
        {
            _isContextMessageSentEvent.Set();
        }
    }
}
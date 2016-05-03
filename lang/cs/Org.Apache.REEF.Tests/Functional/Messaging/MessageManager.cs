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

using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Tests.Functional.Messaging
{
    /// <summary>
    /// A test helper class that keeps track of whether context/task messages 
    /// have been sent.
    /// </summary>
    public sealed class MessageManager
    {
        private readonly object _lock = new object();
        private bool _isContextMessageSent = false;
        private bool _isTaskMessageSent = false;

        [Inject]
        private MessageManager()
        {
        }

        /// <summary>
        /// Returns true if context message has been sent.
        /// </summary>
        public bool IsContextMessageSent
        {
            get
            {
                lock (_lock)
                {
                    return _isContextMessageSent;
                }
            }
        }

        /// <summary>
        /// Returns true if task message has been sent.
        /// </summary>
        public bool IsTaskMessageSent
        {
            get
            {
                lock (_lock)
                {
                    return _isTaskMessageSent;
                }
            }
        }

        /// <summary>
        /// Called when Task message is sent.
        /// </summary>
        public void OnTaskMessageSent()
        {
            lock (_lock)
            {
                _isTaskMessageSent = true;
            }
        }

        /// <summary>
        /// Called when Context message is sent.
        /// </summary>
        public void OnContextMessageSent()
        {
            lock (_lock)
            {
                _isContextMessageSent = true;
            }
        }
    }
}
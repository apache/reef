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
using System.Runtime.Serialization;

namespace Org.Apache.REEF.IMRU.OnREEF.IMRUTasks
{
    /// <summary>
    /// A serializable exception that represents a task group communication error.
    /// </summary>
    [Serializable]
    internal sealed class IMRUTaskGroupCommunicationException : Exception
    {
        /// <summary>
        /// Constructor. A serializable exception object that represents a task group communication error.
        /// All the errors caused by group communication should be wrapped in this type of exception. For example, 
        /// when one of the nodes in the group fails, other nodes are not able to receive messages
        /// therefore fail. We should use this exception type to represent this error.  
        /// When driver receives this type of exception, it is recoverable. 
        /// </summary>
        public IMRUTaskGroupCommunicationException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor. A serializable exception object that represents a task group communication error and wraps an inner exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public IMRUTaskGroupCommunicationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public IMRUTaskGroupCommunicationException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}

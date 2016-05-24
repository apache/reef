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
    /// A serialiable exception that represents a task system error.
    /// </summary>
    [Serializable]
    internal sealed class IMRUTaskSystemException : Exception
    {
        /// <summary>
        /// Constructor. A serializable exception object that represents a task system error.
        /// This exception can be thrown when an exception happens in IMRU task and we would like 
        /// driver to know this type of exception is thrown from IMRU system. 
        /// For example, if when task is enforced to close by driver, this exception type can be used. 
        /// When driver receives this type of exception, it is recoverable. 
        /// </summary>
        public IMRUTaskSystemException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor. A serializable exception object that represents a task system error and wraps an inner exception.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public IMRUTaskSystemException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public IMRUTaskSystemException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
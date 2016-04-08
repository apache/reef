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

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Exception class for the case when Tcp Client connection fails 
    /// after trying for some finite number of times.
    /// For example, see TcpClientConnectionFactory.cs
    /// </summary>
    public sealed class TcpClientConnectionException : Exception
    {
        /// <summary>
        /// Number of retries done before exception was thrown
        /// </summary>
        public int RetriesDone { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="message">The user message for exception</param>
        /// <param name="retriesDone">Number of retries</param>
        public TcpClientConnectionException(string message, int retriesDone)
            : base(message)
        {
            RetriesDone = retriesDone;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="message">The user message for exception</param>
        /// <param name="inner">The actual exception message due to which connection failed</param>
        /// <param name="retriesDone">Number of retries</param>
        public TcpClientConnectionException(string message, Exception inner, int retriesDone)
            : base(message, inner)
        {
            RetriesDone = retriesDone;
        }

        /// <summary>
        /// Appends the number of retries to the exception message.
        /// </summary>
        public override string Message
        {
            get
            {
                return base.Message + string.Format(", RetriesDone={0}", RetriesDone);
            }
        }
    }
}

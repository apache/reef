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
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Type of exception thrown when possible bugs are detected in IMRU code.
    /// For example, we reach forbidden region of codes, inconsistent state etc.
    /// </summary>
    public class IMRUSystemException : Exception
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(IMRUSystemException));

        /// <summary>
        /// Constructor. Appends the user message with the message that 
        /// there is an issue in IMRU code.
        /// </summary>
        /// <param name="message">The user message for exception</param>
        public IMRUSystemException(string message)
            : base(AppendedMessage(message))
        {
        }

        /// <summary>
        /// Constructor. Appends the user message with the message that 
        /// there is an issue in IMRU code. Also throws the provided exception.
        /// </summary>
        /// <param name="message">The user message for exception</param>
        /// <param name="inner">The actual exception message due to which connection failed</param>
        public IMRUSystemException(string message, Exception inner)
            : base(AppendedMessage(message), inner)
        {
        }

        private static string AppendedMessage(string message)
        {
            return "Possible Bug in the IMRU code: " + message;
        }
    }
}

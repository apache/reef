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

namespace Org.Apache.REEF.Wake.Remote
{
    /// <summary>Wake remote runtime exception</summary>
    [System.Serializable]
    public sealed class RemoteRuntimeException : Exception
    {
        private const long serialVersionUID = 1L;

        /// <summary>Constructs a new runtime remote exception with the specified detail message and cause
        ///     </summary>
        /// <param name="s">the detailed message</param>
        /// <param name="e">the cause</param>
        public RemoteRuntimeException(string s, Exception e)
            : base(s, e)
        {
        }

        /// <summary>Constructs a new runtime remote exception with the specified detail message
        ///     </summary>
        /// <param name="s">the detailed message</param>
        public RemoteRuntimeException(string s)
            : base(s)
        {
        }

        /// <summary>Constructs a new runtime remote exception with the specified cause</summary>
        /// <param name="e">the cause</param>
        public RemoteRuntimeException(Exception e)
            : base("Runtime Exception", e)
        {
        }
    }
}

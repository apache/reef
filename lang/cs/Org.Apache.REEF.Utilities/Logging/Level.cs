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

namespace Org.Apache.REEF.Utilities.Logging
{
    public enum Level
    {
        /// <summary>
        /// Output no tracing and debugging messages.
        /// </summary>
        Off = 0,

        /// <summary>
        /// Output error-handling messages.
        /// </summary>
        Error = 1,

        /// <summary>
        /// Output warnings and error-handling messages.
        /// </summary>
        Warning = 2,

        /// <summary>
        /// Trace a start event
        /// </summary>
        Start = 3,

        /// <summary>
        /// Trace a stop event
        /// </summary>
        Stop = 4,

        /// <summary>
        /// Output informational messages, warnings, and error-handling messages.
        /// </summary>
        Info = 5,

        /// <summary>
        /// Output all debugging and tracing messages.
        /// </summary>
        Verbose = 6,
    }
}

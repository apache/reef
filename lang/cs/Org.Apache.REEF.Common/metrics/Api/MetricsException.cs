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
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Metrics.Api
{
    /// <summary>
    /// Exception class used to wrap any exception in metrics.
    /// </summary>
    [Unstable("0.16", "Contract may change.")]
    public class MetricsException : Exception
    {
        // Static message to pre-append to any user message.
        private static readonly string MessagePrefix = "Error in Metrics.";

        /// <summary>
        /// Empty constructor.
        /// </summary>
        public MetricsException() : base(MessagePrefix)
        {
        }

        /// <summary>
        /// Construct exception with the user message.
        /// </summary>
        /// <param name="message">User defined message.</param>
        public MetricsException(string message)
            : base(string.Format("{0} {1}", MessagePrefix, message))
        {
        }

        /// <summary>
        /// Construct exception with the user message and inner exception.
        /// </summary>
        /// <param name="message">User defined message.</param>
        /// <param name="innerException">Inner exception.</param>
        public MetricsException(string message, Exception innerException)
            : base(string.Format("{0} {1}", MessagePrefix, message), innerException)
        {
        }
    }
}

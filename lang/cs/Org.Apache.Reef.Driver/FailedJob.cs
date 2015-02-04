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

using Org.Apache.Reef.Common.Api;
using Org.Apache.Reef.Utilities;
using System;

namespace Org.Apache.Reef.Driver
{
    /// <summary>
    /// An error message that REEF Client receives when there is a user error in REEF job.
    /// </summary>
    public class FailedJob : AbstractFailure
    {
        /// <summary>
        /// Create an error message given the entity ID and Java Exception. All accessor methods are provided by the base class.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="cause"></param>
        public FailedJob(string id, Exception cause)
            : base(id, cause)
        {
        }

        public new string Id { get; set; }

        public new string Message { get; set; }

        public new Optional<string> Description { get; set; }

        public new Optional<Exception> Cause { get; set; }

        public new Optional<byte[]> Data { get; set; }
    }
}

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

using System.Net.Http;

namespace Org.Apache.REEF.Client.YARN.RestClient
{
    /// <summary>
    /// Encapsulated data related to a REST request
    /// </summary>
    internal class RestRequest
    {
        /// <summary>
        /// Path to the resource being accessed
        /// This path is relative to the base path.
        /// </summary>
        public string Resource { get; set; }

        /// <summary>
        /// Root element (if any) in the response JSON
        /// </summary>
        public string RootElement { get; set; }

        /// <summary>
        /// HTTP method to be invoked for the request
        /// </summary>
        public Method Method { get; set; }

        /// <summary>
        /// The serialized string content for the request
        /// </summary>
        public StringContent Content { get; internal set; }
    }
}
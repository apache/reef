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
using System.Net;

namespace Org.Apache.REEF.Client.YARN.RestClient
{
    /// <summary>
    /// Represents the response of a REST request
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class RestResponse<T>
    {
        /// <summary>
        /// Deserialized response for REST request
        /// </summary>
        public T Data { get; set; }

        /// <summary>
        /// StatusCode of the REST response
        /// </summary>
        public HttpStatusCode StatusCode { get; set; }

        /// <summary>
        /// Exception details if a communication/protocol 
        /// error happened while processing the request
        /// </summary>
        public Exception Exception { get; set; }

        /// <summary>
        /// String content (serialized) of the response
        /// </summary>
        public string Content { get; set; }
    }
}
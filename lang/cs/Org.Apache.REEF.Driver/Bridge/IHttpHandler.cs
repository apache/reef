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

namespace Org.Apache.REEF.Driver.Bridge
{
    public interface IHttpHandler
    {
        /// <summary>
        /// Define the specification of the handler. ":" is not allowed in the specification.
        /// </summary>
        /// <returns>string specification</returns>
        string GetSpecification();

        /// <summary>
        /// Called when Http request is sent
        /// </summary>
        /// <param name="request">The request.</param>
        /// <param name="response">The response.</param>
        void OnHttpRequest(ReefHttpRequest requet, ReefHttpResponse resonse);  
    }
}

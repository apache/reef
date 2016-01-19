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

using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// Instantiates the client based on IConfiguration for the chosen runtime.
    /// </summary>
    public static class ClientFactory
    {
        /// <summary>
        /// Creates a new instance of IREEFClient, based on the given Configuration.
        /// </summary>
        /// <remarks>
        /// If the client itself uses Tang, it is a better design to have the IREEFClient injected into it. In order to make
        /// that happen, mix in the appropriate runtime configuration into the client configuration.
        /// </remarks>
        /// <param name="runtimeClientConfiguration">
        /// The client configuration. Typically, this will be created via e.g.
        /// <seealso cref="LocalRuntimeClientConfiguration" />
        /// </param>
        /// <returns></returns>
        public static IREEFClient GetClient(IConfiguration runtimeClientConfiguration)
        {
            return TangFactory.GetTang().NewInjector(runtimeClientConfiguration).GetInstance<IREEFClient>();
        }
    }
}
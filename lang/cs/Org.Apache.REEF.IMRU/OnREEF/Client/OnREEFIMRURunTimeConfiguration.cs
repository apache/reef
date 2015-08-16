﻿/**
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

using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.YARN;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IMRU.OnREEF.Client
{
    /// <summary>
    /// Configuration for Runtime for IMRU on REEF.
    /// </summary>
    /// <typeparam name="TMapInput">The type of the side information provided to the Map function</typeparam>
    /// <typeparam name="TMapOutput">The return type of the Map function</typeparam>
    /// <typeparam name="TResult">The return type of the computation.</typeparam>
    public static class OnREEFIMRURunTimeConfigurations<TMapInput, TMapOutput, TResult>
    {
        /// <summary>
        /// Function that specifies local runtime configuration for IMRU
        /// </summary>
        /// <returns>The local runtime configuration</returns>
        public static IConfiguration GetLocalIMRUConfiguration()
        {
            IConfiguration runtimeConfig =
                LocalRuntimeClientConfiguration.ConfigurationModule.Build();
            return TangFactory.GetTang().NewConfigurationBuilder(runtimeConfig)
                .BindImplementation(GenericType<IIMRUClient<TMapInput, TMapOutput, TResult>>.Class,
                    GenericType<REEFIMRUClient<TMapInput, TMapOutput, TResult>>.Class)
                .Build();
        }

        /// <summary>
        /// Function that specifies yarn runtime configuration for IMRU on the cluster
        /// </summary>
        /// <returns>The yarn runtime configuration</returns>
        public static IConfiguration GetYarnIMRUConfiguration()
        {
            IConfiguration runtimeConfig =
                YARNClientConfiguration.ConfigurationModule.Build();
            return TangFactory.GetTang().NewConfigurationBuilder(runtimeConfig)
                .BindImplementation(GenericType<IIMRUClient<TMapInput, TMapOutput, TResult>>.Class,
                    GenericType<REEFIMRUClient<TMapInput, TMapOutput, TResult>>.Class)
                .Build();
        }
    }
}
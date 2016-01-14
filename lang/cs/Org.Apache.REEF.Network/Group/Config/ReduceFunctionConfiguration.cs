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

using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Network.Group.Config
{
    public sealed class ReduceFunctionConfiguration<T> : ConfigurationModuleBuilder
    {
        /// <summary>
        /// RequiredImpl for Reduced Function. Client needs to set implementation for this paramter
        /// </summary>
        public static readonly RequiredImpl<IReduceFunction<T>> ReduceFunction = new RequiredImpl<IReduceFunction<T>>();
        
        /// <summary>
        /// Configuration Module for Reduced Function
        /// </summary>
        public static ConfigurationModule Conf = new ReduceFunctionConfiguration<T>()
            .BindImplementation(GenericType<IReduceFunction<T>>.Class, ReduceFunction)
            .Build();
    }
}
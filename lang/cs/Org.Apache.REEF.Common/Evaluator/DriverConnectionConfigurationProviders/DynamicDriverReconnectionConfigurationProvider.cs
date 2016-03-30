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
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Common.Evaluator.DriverConnectionConfigurationProviders
{
    /// <summary>
    /// A helper <see cref="IDriverReconnectionConfigurationProvider"/> that binds <see cref="IDriverConnection"/>
    /// dynamically. This is used such that a <see cref="IDriverReconnectionConfigurationProvider"/> can
    /// be used to provide the type of <see cref="IDriverConnection"/> to the Evaluator if the user only
    /// binds the deprecated <see cref="IDriverConnection"/> optional implementation, instead of the
    /// new <see cref="IDriverReconnectionConfigurationProvider"/> implementation.
    /// </summary>
    internal sealed class DynamicDriverReconnectionConfigurationProvider : IDriverReconnectionConfigurationProvider
    {
        private readonly Type _driverReconnectionType;

        internal DynamicDriverReconnectionConfigurationProvider(Type type)
        {
            _driverReconnectionType = type;
            if (!typeof(IDriverConnection).IsAssignableFrom(type))
            {
                throw new ArgumentException("The type must be a subclass of IDriverConnection.");
            }
        }

        /// <summary>
        /// A configuration where the type of <see cref="IDriverConnection"/> is bound dynamically.
        /// </summary>
        public IConfiguration GetConfiguration()
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(typeof(IDriverConnection), _driverReconnectionType)
                .Build();
        }
    }
}
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

using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Network.Group.Driver
{
    /// <summary>
    /// Used to create Communication Groups for Group Communication Operators.
    /// Also manages configuration for Group Communication tasks/services.
    /// The extra function allows user to add configuration to service For example streaming codec 
    /// for messages.
    /// </summary>
    public interface IWritableGroupCommDriver : IGroupCommDriver
    {
        /// <summary>
        /// Get the service configuration required for running Group Communication on Reef tasks.
        /// </summary>
        /// <param name="config">User specified configuration to be passed to service</param>
        /// <returns>The service configuration for the Reef tasks</returns>
        IConfiguration GetServiceConfiguration(params IConfiguration[] config);
    }
}

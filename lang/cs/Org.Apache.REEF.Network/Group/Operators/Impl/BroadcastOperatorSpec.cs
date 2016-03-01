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
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    /// <summary>
    /// The specification used to define Broadcast Operators.
    /// </summary>
    [Private]
    public sealed class BroadcastOperatorSpec : IOperatorSpec
    {
        /// <summary>
        /// Specification for Broadcast Operator
        /// </summary>
        /// <param name="senderId"></param>
        /// <param name="configurations"></param>
        public BroadcastOperatorSpec(string senderId, params IConfiguration[] configurations)
        {
            SenderId = senderId;
            Configuration = Configurations.Merge(configurations);
        }

        /// <summary>
        /// Returns the identifier of the Task that will broadcast data to other Tasks.
        /// </summary>
        public string SenderId { get; private set; }

        /// <summary>
        /// Returns the Configuration for Codec, ReduceFunction and DataConverter
        /// </summary>
        public IConfiguration Configuration { get; private set; }
    }
}

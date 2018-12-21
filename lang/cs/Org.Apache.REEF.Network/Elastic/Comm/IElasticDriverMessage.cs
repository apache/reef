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

using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Comm
{
    /// <summary>
    /// Message sent by the driver to operators on running tasks. 
    /// This message contains instructions from the driver to tasks's operators.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface IElasticDriverMessage
    {
        /// <summary>
        /// The destination task of the message.
        string Destination { get; }

        /// <summary>
        /// Operator and event specific payload of the message.
        /// </summary>
        DriverMessagePayload Message { get; }

        /// <summary>
        /// Utility method to serialize the message for communication over the network.
        /// </summary>
        /// <returns>The serialized message</returns>
        byte[] Serialize();
    }
}
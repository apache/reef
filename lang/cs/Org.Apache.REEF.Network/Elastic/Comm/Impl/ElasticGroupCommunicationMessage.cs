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

using Org.Apache.REEF.Network.Elastic.Task;
using Org.Apache.REEF.Utilities.Attributes;
using System;

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Message sent by Group Communication operators. 
    /// </summary>
    [Unstable("0.16", "API may change")]
    public abstract class ElasticGroupCommunicationMessage : ICloneable, INodeIdentifier
    {
        /// <summary>
        /// Create a new elastic group communication message.
        /// </summary>
        /// <param name="stageName">The name of the stage</param>
        /// <param name="operatorId">The id of the operator sending the message</param>
        protected ElasticGroupCommunicationMessage(
            string stageName,
            int operatorId)
        {
            StageName = stageName;
            OperatorId = operatorId;
        }

        /// <summary>
        /// Clone the message.
        /// </summary>
        public abstract object Clone();

        /// <summary>
        /// Returns the stage.
        public string StageName { get; }

        /// <summary>
        /// Returns the operator id.
        /// </summary>
        public int OperatorId { get; }
    }
}
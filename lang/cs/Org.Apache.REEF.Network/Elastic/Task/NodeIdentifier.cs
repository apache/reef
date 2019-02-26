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

namespace Org.Apache.REEF.Network.Elastic.Task
{
    /// <summary>
    /// An identifier for a given node in the group communication topology.
    /// A node is uniquely identifiable by a combination of its 
    /// <see cref="StageName"/>, and <see cref="OperatorId"/>.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public struct NodeIdentifier : INodeIdentifier
    {
        /// <summary>
        /// The stage name.
        /// </summary>
        public string StageName { get; private set; }

        /// <summary>
        /// The operator name.
        /// </summary>
        public int OperatorId { get; private set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="stageName"></param>
        /// <param name="operatorId"></param>
        public NodeIdentifier(string stageName, int operatorId)
        {
            StageName = stageName;
            OperatorId = operatorId;
        }
    }
}
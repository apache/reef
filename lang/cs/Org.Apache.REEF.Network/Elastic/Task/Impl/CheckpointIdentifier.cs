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

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// An identifier for a given node in the group communication graph.
    /// A node is uniquely identifiable by a combination of its Task ID, 
    /// <see cref="StageName"/>, and <see cref="OperatorName"/>.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class CheckpointIdentifier
    {
        /// <summary>
        /// Construct a new checkpoint identifier.
        /// </summary>
        /// <param name="stageName">The stage name</param>
        /// <param name="operatorId">The operator identifier</param>
        public CheckpointIdentifier(string stageName, int operatorId)
        {
            StageName = stageName;
            OperatorId = operatorId;
        }

        /// <summary>
        /// The stage name of the node.
        /// </summary>
        public string StageName { get; private set; }

        /// <summary>
        /// The operator id of the node.
        /// </summary>
        public int OperatorId { get; private set; }

        /// <summary>
        /// Overrides <see cref="Equals"/>. Simply compares equivalence of instance fields.
        /// </summary>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return obj is CheckpointIdentifier && Equals((CheckpointIdentifier)obj);
        }

        /// <summary>
        /// Overrides <see cref="GetHashCode"/>. Generates hashcode based on the instance fields.
        /// </summary>
        public override int GetHashCode()
        {
            int hash = 17;
            hash = (hash * 31) + StageName.GetHashCode();
            return (hash * 31) + OperatorId.GetHashCode();
        }

        /// <summary>
        /// Compare equality of instance fields.
        /// </summary>
        private bool Equals(CheckpointIdentifier other)
        {
            return StageName.Equals(other.StageName) &&
                OperatorId.Equals(other.OperatorId);
        }
    }
}
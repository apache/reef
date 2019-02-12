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

using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Physical;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// An identifier for a given node in the group communication topology.
    /// A node is uniquely identifiable by a combination of its 
    /// <see cref="StageName"/>, and <see cref="OperatorId"/>.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class NodeObserverIdentifier
    {
         /// <summary>
        /// Creates an identifier from an operator topology with communication.
        /// </summary>
        public static NodeObserverIdentifier FromObserver(IOperatorTopologyWithCommunication observer)
        {
            return new NodeObserverIdentifier(observer.StageName, observer.OperatorId);
        }

        /// <summary>
        /// Creates an from a driver aware topology.
        /// </summary>
        public static NodeObserverIdentifier FromObserver(DriverAwareOperatorTopology observer)
        {
            return new NodeObserverIdentifier(observer.StageName, observer.OperatorId);
        }

        /// <summary>
        /// Creates an identifier from a group communication message.
        /// </summary>
        public static NodeObserverIdentifier FromMessage(ElasticGroupCommunicationMessage message)
        {
            return new NodeObserverIdentifier(message.StageName, message.OperatorId);
        }

        /// <summary>
        /// Basic constructor.
        /// </summary>
        /// <param name="stageName">The name of the stage</param>
        /// <param name="operatorId">The identifier of the operator</param>
        private NodeObserverIdentifier(string stageName, int operatorId)
        {
            StageName = stageName;
            OperatorId = operatorId;
        }

        /// <summary>
        /// The stage name.
        /// </summary>
        public string StageName { get; }

        /// <summary>
        /// The operator name.
        /// </summary>
        public int OperatorId { get; }

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

            return obj is NodeObserverIdentifier && Equals((NodeObserverIdentifier)obj);
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
        private bool Equals(NodeObserverIdentifier other)
        {
            return StageName.Equals(other.StageName) &&
                OperatorId.Equals(other.OperatorId);
        }
    }
}
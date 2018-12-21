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

using Org.Apache.REEF.Tang.Annotations;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Config
{
    ///<summary>
    ///Class wrapping the configuration option parameters for operators.
    ///</summary>
    public sealed class OperatorParameters
    {
        [NamedParameter("Operator Name")]
        public sealed class OperatorType : Name<string>
        {
        }

        [NamedParameter("Type of the message")]
        public sealed class MessageType : Name<string>
        {
        }

        [NamedParameter("Operator Id")]
        public sealed class OperatorId : Name<int>
        {
        }

        [NamedParameter("Name of the subscriptions")]
        public sealed class SubscriptionName : Name<string>
        {
        }

        [NamedParameter("Serialized operator configuration")]
        public sealed class SerializedOperatorConfigs : Name<IList<string>>
        {
        }

        [NamedParameter("Request topology update", defaultValue: "false")]
        public sealed class RequestTopologyUpdate : Name<bool>
        {
        }

        [NamedParameter("Number of iterations")]
        public sealed class NumIterations : Name<int>
        {
        }

        [NamedParameter("Number of element to scatter for each node", defaultValue: "0")]
        public sealed class NumScatterElements : Name<int>
        {
        }

        [NamedParameter("Iteration number to begin with", defaultValue: "1")]
        public sealed class StartIteration : Name<int>
        {
        }

        [NamedParameter("Master Id")]
        public sealed class MasterId : Name<int>
        {
        }

        [NamedParameter("Checkpoint level", defaultValue: "0")]
        public sealed class Checkpointing : Name<int>
        {
        }

        [NamedParameter("Whether the operator is the last to be executed in the subscription", defaultValue: "false")]
        public sealed class IsLast : Name<bool>
        {
        }

        [NamedParameter("Id of root task in operator topology", defaultValue: "-1")]
        public sealed class TopologyRootTaskId : Name<int>
        {
        }

        [NamedParameter("Ids of child tasks in operator topology")]
        public sealed class TopologyChildTaskIds : Name<ISet<int>>
        {
        }

        [NamedParameter("Whether topology updates can be piggybacked to data messages", defaultValue: "false")]
        public sealed class PiggybackTopologyUpdates : Name<bool>
        {
        }
    }
}

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

namespace Org.Apache.REEF.Network.Examples.GroupCommunication
{
    public class GroupTestConfig
    {
        [NamedParameter("Number of iterations of messages to send")]
        public class NumIterations : Name<int>
        {
        }

        [NamedParameter("Number of Evaluators")]
        public class NumEvaluators : Name<int>
        {
        }

        [NamedParameter("tree width")]
        public class FanOut : Name<int>
        {
        }

        [NamedParameter("integer id of the evaluator")]
        public class EvaluatorId : Name<string>
        {
        }

        [NamedParameter(Documentation = "Size of the array", DefaultValue = "6")]
        public class ArraySize : Name<int>
        {
        }

        [NamedParameter(Documentation = "Chunk size for pipelining", DefaultValue = "2")]
        public class ChunkSize : Name<int>
        {
        }

        [NamedParameter(Documentation = "Starting port for TcpPortProvider", DefaultValue = "8900")]
        public class StartingPort : Name<int>
        {
        }

        [NamedParameter(Documentation = "Port Range count for TcpPortProvider", DefaultValue = "1000")]
        public class PortRange : Name<int>
        {
        }
    }
}

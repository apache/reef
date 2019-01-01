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
using Org.Apache.REEF.Network.Elastic.Task;
using Org.Apache.REEF.Network.NetworkService;
using System;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical
{
    /// <summary>
    /// Base interface for topologies where nodes communicate betwen themselves.
    /// </summary>
    internal interface IOperatorTopologyWithCommunication :
        IWaitForTaskRegistration,
        IDisposable,
        IObserver<NsMessage<ElasticGroupCommunicationMessage>>
    {
        /// <summary>
        /// The stage name context in which the topology is running.
        /// </summary>
        string StageName { get; }

        /// <summary>
        /// The identifier of the operator in which the topology is running.
        /// </summary>
        int OperatorId { get; }
    }
}

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

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Message sent to checkpoint service to retrieve a remote checkpoint.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class CheckpointMessageRequest : ElasticGroupCommunicationMessage
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="stageName">The stage name ffor the checkpoint to retrieve</param>
        /// <param name="operatorId">The operator identifier</param>
        /// <param name="iteration">The iteration of the checkpoint of interest</param>
        public CheckpointMessageRequest(
           string stageName,
           int operatorId,
           int iteration) : base(stageName, operatorId)
        {
            Iteration = iteration;
        }

        /// <summary>
        /// Iteration number for the checkpoint of interest.
        /// </summary>
        public int Iteration { get; set; }

        /// <summary>
        /// Clone the message.
        /// </summary>
        public override object Clone()
        {
            return new CheckpointMessageRequest(StageName, OperatorId, Iteration);
        }
    }
}
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

using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Default
{
    /// <summary>
    /// Driver-side broadcast operator implementation.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class DefaultBroadcast<T> : DefaultOneToN<T>, IElasticBroadcast
    {
        /// <summary>
        /// Constructor for a driver-side broadcast opearator.
        /// </summary>
        /// <param name="senderId">The identifier of the sender task</param>
        /// <param name="prev">The previous operator in the pipeline</param>
        /// <param name="topology">The topology for the broadcast operation</param>
        /// <param name="failureMachine">The failure machine managing the failures for the operator</param>
        /// <param name="checkpointLevel">The checkpoint level</param>
        /// <param name="configurations">Additional configurations for the operator</param>
        public DefaultBroadcast(
            int senderId,
            ElasticOperator prev,
            ITopology topology,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel,
            params IConfiguration[] configurations) : base(
                senderId,
                prev,
                topology,
                failureMachine,
                checkpointLevel,
                configurations)
        {
            OperatorType = OperatorType.Broadcast;
        }

        /// <summary>
        /// Generate the data serializer configuration for the target operator.
        /// </summary>
        /// <param name="confBuilder">The conf builder where to attach the codec configuration</param>
        internal override void GetCodecConfiguration(ref IConfiguration conf)
        {
            if (CODECMAP.TryGetValue(typeof(T), out IConfiguration codecConf))
            {
                conf = Configurations.Merge(conf, codecConf);
                base.GetCodecConfiguration(ref conf);
            }
            else
            {
                throw new IllegalStateException($"Codec for type {typeof(T)} not found.");
            }
        }

        /// <summary>
        /// Binding from logical to physical operator.
        /// </summary>
        /// <param name="confBuilder">The configuration builder the binding will be added to</param>
        /// <returns>The physcal operator configurations</returns>
        protected override IConfiguration PhysicalOperatorConfiguration()
        {
            var physicalOperatorConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IElasticTypedOperator<T>>.Class, GenericType<Physical.Default.DefaultBroadcast<T>>.Class)
                .Build();
            var messageconf = SetMessageType(typeof(Physical.Default.DefaultBroadcast<T>));

            return Configurations.Merge(physicalOperatorConf, messageconf);
        }
    }
}
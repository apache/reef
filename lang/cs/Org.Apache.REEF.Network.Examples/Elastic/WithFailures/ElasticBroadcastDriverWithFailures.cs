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

using System;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Enum;
using Org.Apache.REEF.Network.Elastic.Driver.Default;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Failures.Default;
using Org.Apache.REEF.Network.Elastic.Task.Default;
using Org.Apache.REEF.Network.Elastic.Config;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    /// <summary>
    /// Example implementation of broadcasting using the elastic group communication service.
    /// </summary>
    public sealed class ElasticBroadcastDriverWithFailures<TSlave>
        : DefaultElasticDriver
        where TSlave : DefaultElasticTask
    {
        [Inject]
        private ElasticBroadcastDriverWithFailures(
            [Parameter(typeof(ElasticServiceConfigurationOptions.DefaultStageName))] string stageName,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumEvaluators))] int numEvaluators,
            IElasticContext context) : base(context)
        {
            IFailureStateMachine failureMachine = new DefaultFailureStateMachine();

            failureMachine.SetThresholds(
                DefaultFailureState.Threshold(DefaultFailureStates.ContinueAndReconfigure, 0.01F),
                DefaultFailureState.Threshold(DefaultFailureStates.ContinueAndReschedule, 0.40F),
                DefaultFailureState.Threshold(DefaultFailureStates.StopAndReschedule, 0.60F),
                DefaultFailureState.Threshold(DefaultFailureStates.Fail, 0.80F));

            IElasticStage stage = Context.CreateNewStage(stageName, numEvaluators, failureMachine);

            // Create and build the pipeline
            stage.PipelineRoot
                .Broadcast<int>(TopologyType.Flat)
                .Build();

            // Build the stage
            stage = stage.Build();

            // Create the task manager, register the stage to the task manager, build the task set manager
            TaskSetManager = Context
                .CreateNewTaskSetManager(MasterTaskConfiguration, SlaveTaskConfiguration)
                .AddStage(stage)
                .Build();
        }

        private IConfiguration MasterTaskConfiguration(string taskId)
        {
            return Context.GetTaskConfigurationModule(taskId)
                .Set(TaskConfiguration.Task, GenericType<BroadcastMasterTask>.Class)
                .Build();
        }

        private IConfiguration SlaveTaskConfiguration(string taskId)
        {
            return Context.GetTaskConfigurationModule(taskId)
                .Set(TaskConfiguration.Task, GenericType<TSlave>.Class)
                .Build();
        }
    }
}
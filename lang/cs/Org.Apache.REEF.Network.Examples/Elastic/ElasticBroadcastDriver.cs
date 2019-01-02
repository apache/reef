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
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Network.Elastic.Operators.Logical;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    /// <summary>
    /// Example implementation of broadcasting using the elastic group communication service.
    /// </summary>
    public class ElasticBroadcastDriver : DefaultElasticDriver
    {
        [Inject]
        private ElasticBroadcastDriver(IElasticContext context) : base(context)
        {
            Func<string, IConfiguration> masterTaskConfiguration = (taskId) => TangFactory.GetTang().NewConfigurationBuilder(
                Context.GetTaskConfigurationModule(taskId)
                    .Set(TaskConfiguration.Task, GenericType<BroadcastMasterTask>.Class)
                    .Build())
                .Build();

            Func<string, IConfiguration> slaveTaskConfiguration = (taskId) => TangFactory.GetTang().NewConfigurationBuilder(
                Context.GetTaskConfigurationModule(taskId)
                    .Set(TaskConfiguration.Task, GenericType<BroadcastSlaveTask>.Class)
                    .Build())
                .Build();

            IElasticStage stage = Context.DefaultStage();

            ElasticOperator pipeline = stage.RootOperator;

            // Create and build the pipeline
            pipeline.Broadcast<int>(TopologyType.Flat)
                    .Build();

            // Build the stage
            stage = stage.Build();

            // Create the task manager
            TaskSetManager = Context.CreateNewTaskSetManager(masterTaskConfiguration, slaveTaskConfiguration);

            // Register the stage to the task manager
            TaskSetManager.AddStage(stage);

            // Build the task set manager
            TaskSetManager.Build();
        }
    }
}

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
using static Org.Apache.REEF.Network.Elastic.Config.ElasticServiceConfigurationOptions;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    /// <summary>
    /// Example implementation of broadcasting using the elastic group communication service.
    /// </summary>
    public sealed class ElasticBroadcastDriverWithFailureBeforeBroadcast :
        ElasticBroadcastDriverWithFailures
    {
        [Inject]
        private ElasticBroadcastDriverWithFailureBeforeBroadcast(
            [Parameter(typeof(DefaultStageName))] string stageName,
            [Parameter(typeof(NumEvaluators))] int numEvaluators,
            IElasticContext context) : base(stageName, numEvaluators, context)
        {
        }

        protected override Func<string, IConfiguration> SlaveTaskConfiguration
        {
            get
            {
                return (taskId) => TangFactory.GetTang().NewConfigurationBuilder(
                    Context.GetTaskConfigurationModule(taskId)
                        .Set(
                            TaskConfiguration.Task,
                            GenericType<BroadcastSlaveTaskDieBeforeBroadcast>.Class)
                        .Build())
                    .Build();
            }
        }
    }
}
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
using Org.Apache.REEF.Network.Elastic.Config;
using System.Threading.Tasks;
using Org.Apache.REEF.Network.Elastic.Failures;

namespace Org.Apache.REEF.Network.Elastic.Driver.Default
{
    /// <summary>
    /// Injectable class containing all the parameters for the default task set manager.
    /// </summary>
    internal sealed class DefaultElasticTaskSetManagerParameters
    {
        [Inject]
        private DefaultElasticTaskSetManagerParameters(
            FailuresClock clock,
            [Parameter(typeof(ElasticServiceConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(ElasticServiceConfigurationOptions.SendRetry))] int retry,
            [Parameter(typeof(ElasticServiceConfigurationOptions.RetryWaitTime))] int waitTime,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumTaskFailures))] int numTaskFailures,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumEvaluatorFailures))] int numEvaluatorFailures,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorRackName))] string rackName,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorBatchId))] string batchId,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorNumCores))] int numCores,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorMemorySize))] int memorySize)
        {
            Clock = clock;
            Timeout = timeout;
            Retry = retry;
            WaitTime = waitTime;
            NumTaskFailures = numTaskFailures;
            NumEvaluatorFailures = numEvaluatorFailures;
            NewEvaluatorRackName = rackName;
            NewEvaluatorBatchId = batchId;
            NewEvaluatorNumCores = numCores;
            NewEvaluatorMemorySize = memorySize;

            System.Threading.Tasks.Task.Factory.StartNew(() => Clock.Run(), TaskCreationOptions.LongRunning);
        }

        /// <summary>
        /// The clock for scheduling alarms.
        /// </summary>
        public FailuresClock Clock { get; private set; }

        /// <summary>
        /// Timeout after which computation is considered inactive.
        /// </summary>
        public int Timeout { get; private set; }

        /// <summary>
        /// How many times a message communication can be retried.
        /// </summary>
        public int Retry { get; private set; }

        /// <summary>
        /// How much time to wait between messages retry.
        /// </summary>
        public int WaitTime { get; private set; }

        /// <summary>
        /// Supported number of task failures.
        /// </summary>
        public int NumTaskFailures { get; private set; }

        /// <summary>
        /// Supported number of evaluator failures.
        /// </summary>
        public int NumEvaluatorFailures { get; private set; }

        /// <summary>
        /// The rack name when spawning new evaluators.
        /// </summary>
        public string NewEvaluatorRackName { get; private set; }

        /// <summary>
        /// The batch id when spawning new evaluators.
        /// </summary>
        public string NewEvaluatorBatchId { get; private set; }

        /// <summary>
        /// Number of cores for new evaluators.
        /// </summary>
        public int NewEvaluatorNumCores { get; private set; }

        /// <summary>
        /// Memory size for new evaluators.
        /// </summary>
        public int NewEvaluatorMemorySize { get; private set; }
    }
}

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

using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce;
using TaskIdsToFail = Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce.FaultTolerantPipelinedBroadcastAndReduce.TaskIdsToFail;
using FailureType = Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce.FaultTolerantPipelinedBroadcastAndReduce.FailureType;
using TestSenderMapFunction = Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce.FaultTolerantPipelinedBroadcastAndReduce.TestSenderMapFunction;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public sealed class TestFailMapperTasks : TestFailMapperEvaluators
    {
        /// <summary>
        /// This test throws exception in two tasks during task execution stage. 
        /// This is classified as task app failure, so no retries are done, and job fails.
        /// </summary>
        [Fact]
        public override void TestFailedMapperOnLocalRuntime()
        {
            int chunkSize = 2;
            int dims = 100;
            int iterations = 200;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 9;
            string testFolder = DefaultRuntimeFolder + TestId;
            TestBroadCastAndReduce(false,
                numTasks,
                chunkSize,
                dims,
                iterations,
                mapperMemory,
                updateTaskMemory,
                NumberOfRetry,
                testFolder);
            string[] lines = ReadLogFile(DriverStdout, "driver", testFolder, 240);
            var completedTaskCount = GetMessageCount(lines, "Received ICompletedTask");
            var runningTaskCount = GetMessageCount(lines, RunningTaskMessage);
            var failedEvaluatorCount = GetMessageCount(lines, FailedEvaluatorMessage);
            var failedTaskCount = GetMessageCount(lines, FailedTaskMessage);
            var jobFailure = GetMessageCount(lines, FailActionMessage);

            // each task should fail or complete
            // there should be no failed evaluators
            // all tasks should start successfully
            // No retry is done because IMRUAppTaskException is triggered by the failure in map task execution.
            Assert.Equal(numTasks, completedTaskCount + failedTaskCount);
            Assert.Equal(0, failedEvaluatorCount);
            Assert.Equal(numTasks, runningTaskCount);
            
            // job fails
            Assert.True(jobFailure > 0);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Mapper function configuration. Subclass can override it to have its own test function.
        /// </summary>
        /// <returns></returns>
        protected override IConfiguration BuildMapperFunctionConfig()
        {
            var c = IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<TestSenderMapFunction>.Class)                   
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(c)
                .BindSetEntry<TaskIdsToFail, string>(GenericType<TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-2-")
                .BindSetEntry<TaskIdsToFail, string>(GenericType<TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-3-")
                .BindIntNamedParam<FailureType>(FailureType.TaskFailureDuringTaskExecution.ToString())
                .BindNamedParameter(typeof(MaxRetryNumberInRecovery), NumberOfRetry.ToString())
                .BindNamedParameter(typeof(FaultTolerantPipelinedBroadcastAndReduce.TotalNumberOfForcedFailures), NumberOfRetry.ToString())
                .Build();
        }
    }
}
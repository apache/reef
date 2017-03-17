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
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;
using TestSenderMapFunction = Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce.PipelinedBroadcastAndReduceWithFaultTolerant.SenderMapFunctionFT;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public sealed class TestFailMapperEvaluatorsOnDispose : TestFailMapperEvaluators
    {
        /// <summary>
        /// This test fails two evaluators during task dispose stage. 
        /// The failures are ignored, because tasks are already completed successfully.
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
            string[] lines = ReadLogFile(DriverStdout, "driver", testFolder, 360);
            var failedEvaluatorCount = GetMessageCount(lines, FailedEvaluatorMessage);
            var failedTaskCount = GetMessageCount(lines, FailedTaskMessage);
            var jobSuccess = GetMessageCount(lines, DoneActionMessage);

            // In this test one of evaluators fails at task dispose stage. Depending on the timing of the failure,
            // if it happens after all tasks completed, the job succeeds immediately,
            // but if it happens before that, this counts as failure and job restarts.
            // Number of tries done can be detected as number of recoveries done + 1
            var triesDone = GetMessageCount(lines, "Start recovery") + 1;

            // There should be no failed tasks.
            // Number of failed evaluators = number of tries done
            // Can't say anything about the number of completed tasks (depends on timing)
            Assert.Equal(triesDone, failedEvaluatorCount);
            Assert.Equal(0, failedTaskCount);

            // but eventually job must succeed
            Assert.Equal(1, jobSuccess);
            CleanUp(testFolder);
        }

        protected override IConfiguration BuildMapperFunctionConfig()
        {
            var c = IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<TestSenderMapFunction>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(c)
                .BindSetEntry<PipelinedBroadcastAndReduceWithFaultTolerant.TaskIdsToFail, string>(GenericType<PipelinedBroadcastAndReduceWithFaultTolerant.TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-2-")
                .BindIntNamedParam<PipelinedBroadcastAndReduceWithFaultTolerant.FailureType>(PipelinedBroadcastAndReduceWithFaultTolerant.FailureType.EvaluatorFailureDuringTaskDispose.ToString())
                .BindNamedParameter(typeof(MaxRetryNumberInRecovery), NumberOfRetry.ToString())
                .BindNamedParameter(typeof(PipelinedBroadcastAndReduceWithFaultTolerant.TotalNumberOfForcedFailures), NumberOfRetry.ToString())
                .Build();
        }
    }
}
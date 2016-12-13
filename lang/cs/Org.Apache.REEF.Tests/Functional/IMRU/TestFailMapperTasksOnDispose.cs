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
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;
using TestSenderMapFunction = Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce.PipelinedBroadcastAndReduceWithFaultTolerant.SenderMapFunctionFT;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public sealed class TestFailMapperTasksOnDispose : TestFailMapperEvaluators
    {
        /// <summary>
        /// This test fails two tasks during task dispose stage. 
        /// The failures are ignored on core REEF layer, so no failed task events are received.
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
            var completedTaskCount = GetMessageCount(lines, "Received ICompletedTask");
            var failedEvaluatorCount = GetMessageCount(lines, FailedEvaluatorMessage);
            var failedTaskCount = GetMessageCount(lines, FailedTaskMessage);
            var jobSuccess = GetMessageCount(lines, IMRUDriver<int[], int[], int[], int[]>.DoneActionPrefix);

            // In this test one of evaluators fails at task dispose stage. Depending on the timing of the failure,
            // if it happens after all tasks completed, the job succeeds immediately,
            // but if it happens before that, this counts as failure and job restarts.
            // Number of tries done can be detected as number of recoveries done + 1
            var triesDone = GetMessageCount(lines, "Start recovery") + 1;

            // When Task.Dispose throws exception, it will result in evaluator failure. 
            // But if master task has completed earlier and this evaluator is closed by the driver, 
            // this evaluator may not be able to send IFailedEvaluator event back to driver
            // "WARNING: Evaluator trying to schedule a heartbeat after a completed heartbeat has already been scheduled or sent."
            Assert.True(triesDone >= failedEvaluatorCount);

            // All the retries can only be triggered by failed evaluator
            // But not all failed evaluators trigger retry (if tasks completed before failure, the job will succeed)
            Assert.True(failedEvaluatorCount >= triesDone - 1);

            // Scenario1: Driver receives FailedEvaluator caused by dispose of a completed task after all the tasks have been competed. 
            //            FailedEvaluator event will be ignored.
            // Scenario2: Driver receives FailedEvaluator caused by dispose of a completed task before all the tasks have been competed.
            //            Driver will send close event to the rest of the running tasks and enter shutdown then recovery. 
            //            During this process, some tasks can still complete and some may fail due to communication error
            //            As evaluator failure happens in finally block, therefore either ICompletedTask or IFailedTask event should be sent before it.
            //            Considering once maser is done, rest of the contexts will be disposed, we have 
            //            completedTask# + FailedTask# <= numTasks
            Assert.True(triesDone * numTasks >= completedTaskCount + failedTaskCount);

            // eventually job succeeds
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
                .BindIntNamedParam<PipelinedBroadcastAndReduceWithFaultTolerant.FailureType>(PipelinedBroadcastAndReduceWithFaultTolerant.FailureType.TaskFailureDuringTaskDispose.ToString())
                .BindNamedParameter(typeof(MaxRetryNumberInRecovery), NumberOfRetry.ToString())
                .BindNamedParameter(typeof(PipelinedBroadcastAndReduceWithFaultTolerant.TotalNumberOfForcedFailures), NumberOfRetry.ToString())
                .Build();
        }
    }
}
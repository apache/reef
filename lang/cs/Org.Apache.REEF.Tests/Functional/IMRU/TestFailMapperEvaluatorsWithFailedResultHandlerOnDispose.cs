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
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public class TestFailMapperEvaluatorsWithFailedResultHandlerOnDispose : TestFailMapperEvaluators
    {
        /// <summary>
        /// This test fails two mappers during the iterations. When driver is to close master task, 
        /// the ResultHandler in the Update task will throw exception in Dispose.
        /// This Dispose can be called either when the task is returned from Call() by cancellation token, then TaskRuntime calls Dispose
        /// or by the finally block in TaskRuntime.Close() method, depending on which one is quicker. 
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
            var runningTaskCount = GetMessageCount(lines, RunningTaskMessage);

            // As the driver will shut down as soon as all tasks are in final state. The task state is final either by 
            // ICompletedTask or IFailedEvaluator. But MessageLogger may not be able to receive the last event 
            // before driver shut down. 
            var failedEvaluatorCount = GetMessageCount(lines, FailedEvaluatorMessage);
            var completedTaskCount = GetMessageCount(lines, CompletedTaskMessage);

            var failedTaskCount = GetMessageCount(lines, FailedTaskMessage);
            var jobSuccess = GetMessageCount(lines, DoneActionMessage);

            // All tasks should start running before fail
            Assert.Equal(numTasks, runningTaskCount);

            // Tasks should fail or complete or disappear with failed evaluator
            // However, if the updateTask is completed before receiving Close event from the driver, 
            // TaskRuntime informs the driver about the result before throwing exception caused by the dispose in 
            // the result handler, that may end up two events from the update task/evaluator, one is CompletedTask, 
            // the other is FailedEvaluator
            var total = completedTaskCount + failedEvaluatorCount + failedTaskCount;
            var faildMsg = string.Format(
                "Expected total events: {0}, actual number of events {1}, completedTaskCount: {2}, failedEvaluatorCount: {3}, failedTaskCount: {4}.",
                numTasks,
                total,
                completedTaskCount,
                failedEvaluatorCount,
                failedTaskCount);
            Assert.True(numTasks == total || numTasks == total - 1, faildMsg);
                
            // We have failed two mappers and one update evaluator in the test code. As the update evaluator failure 
            // happens in Dispose(), driver may/may not receive FailedEvaluator before shut down.
            Assert.True(failedEvaluatorCount <= 3 && failedEvaluatorCount >= 2, "failedEvaluatorCount is " + failedEvaluatorCount);

            // eventually job fails because master evaluator fails before the iteration is completed
            Assert.Equal(0, jobSuccess);
            CleanUp(testFolder);
        }

        [Fact]
        [Trait("Environment", "Yarn")]
        public override void TestFailedMapperOnYarn()
        {
            int chunkSize = 2;
            int dims = 100;
            int iterations = 200;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 4;
            TestBroadCastAndReduce(true, numTasks, chunkSize, dims, iterations, mapperMemory, updateTaskMemory);
        }

        /// <summary>
        /// Bind TestExceptionInResultHandlerDispose as IIMRUResultHandler
        /// </summary>
        protected override IConfiguration BuildResultHandlerConfig()
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
                    .BindImplementation(GenericType<IIMRUResultHandler<int[]>>.Class, GenericType<TestExceptionInResultHandlerDispose.ResultHandlerWithException<int[]>>.Class)
                    .Build();
        }
    }
}

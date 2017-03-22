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
using System.Diagnostics;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public class TestExceptionInResultHandlerDispose : IMRUBrodcastReduceTestBase
    {
        /// <summary>
        /// This test is to throw exception in IIMRUResultHandler Dispose() method
        /// </summary>
        [Fact]
        public void TestExceptionInResultHandlerDisposeOnLocalRuntime()
        {
            int chunkSize = 2;
            int dims = 10;
            int iterations = 10;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 4;
            int numberOfRetryInRecovery = 4;

            string testFolder = DefaultRuntimeFolder + TestId;
            TestBroadCastAndReduce(false, numTasks, chunkSize, dims, iterations, mapperMemory, updateTaskMemory, numberOfRetryInRecovery, testFolder);
            string[] lines = ReadLogFile(DriverStdout, "driver", testFolder, 120);
            var completedTaskCount = GetMessageCount(lines, CompletedTaskMessage);
            var failedEvaluatorCount = GetMessageCount(lines, FailedEvaluatorMessage);
            var failedTaskCount = GetMessageCount(lines, FailedTaskMessage);
            var jobSuccess = GetMessageCount(lines, DoneActionMessage);

            // Master evaluator will fail after master task is completed. Depending on how quick the driver dispose contexts after the master task complete,
            // driver may or may not receive the IFailedEvalautor event. 
            Assert.True(failedEvaluatorCount <= 1);

            // Scenario1: Driver receives FailedEvaluator caused by dispose of a completed task after all the tasks have been competed. 
            //            FailedEvaluator event will be ignored.
            // Scenario2: Driver receives FailedEvaluator caused by dispose of master task before all the tasks have been competed.
            //            Driver will send close event to the rest of the running tasks and enter shutdown state 
            //            During this process, some tasks can still complete and some may fail due to communication error
            //            As evaluator failure happens in finally block, therefore either ICompletedTask or IFailedTask event should be sent before it.
            //            Considering once maser is done, rest of the contexts will be disposed, we have 
            //            numTasks >= completedTask# + FailedTask#
            Assert.True(numTasks >= completedTaskCount + failedTaskCount);

            // As update task completion happens before update evaluator failure caused by dispose, eventually job should succeeds
            Assert.Equal(1, jobSuccess);

            CleanUp(testFolder);
        }

        /// <summary>
        /// This test is for the normal scenarios of IMRUDriver and IMRUTasks on yarn
        /// </summary>
        [Fact]
        [Trait("Environment", "Yarn")]
        public void TestExceptionInResultHandlerDisposerOnYarn()
        {
            int chunkSize = 2;
            int dims = 10;
            int iterations = 10;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 4;
            int numberOfRetryInRecovery = 4;
            TestBroadCastAndReduce(false, numTasks, chunkSize, dims, iterations, mapperMemory, updateTaskMemory, numberOfRetryInRecovery);
        }

        /// <summary>
        /// This method defines event handlers for driver. As default, it uses all the handlers defined in IMRUDriver.
        /// </summary>
        protected override IConfiguration DriverEventHandlerConfigurations<TMapInput, TMapOutput, TResult, TPartitionType>()
        {
            return REEF.Driver.DriverConfiguration.ConfigurationModule
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorAllocated,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnDriverStarted,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnContextActive,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskCompleted,
                     GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorFailed,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnContextFailed,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskRunning,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskFailed,
                     GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.CustomTraceLevel, TraceLevel.Info.ToString())
                .Build();
        }

        /// <summary>
        /// Bind ResultHandlerWithException as IIMRUResultHandler
        /// </summary>
        protected override IConfiguration BuildResultHandlerConfig()
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
                    .BindImplementation(GenericType<IIMRUResultHandler<int[]>>.Class, GenericType<ResultHandlerWithException<int[]>>.Class)
                    .Build();
        }

        /// <summary>
        /// An implementation of IIMRUResultHandler that throws exception in Dispose()
        /// </summary>
        internal sealed class ResultHandlerWithException<TResult> : IIMRUResultHandler<TResult>
        {
            [Inject]
            private ResultHandlerWithException()
            {
            }

            /// <summary>
            /// Specifies how to handle the IMRU results from UpdateTask. Does nothing
            /// </summary>
            /// <param name="result">The result of IMRU</param>
            public void HandleResult(TResult result)
            {
            }

            /// <summary>
            /// Simulate exception 
            /// </summary>
            public void Dispose()
            {
                Logger.Log(Level.Warning, "Simulate exception in ResultHandlerWithException.Dispose.");
                throw new Exception("Exception from ResultHandlerWithException.Dispose()");
            }
        }
    }
}
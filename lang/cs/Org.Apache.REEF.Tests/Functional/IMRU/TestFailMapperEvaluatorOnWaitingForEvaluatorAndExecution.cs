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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;
using KillEvaluatorContextStartHandler = Org.Apache.REEF.Tests.Functional.IMRU.TestFailUpdateEvaluatorOnWaitingForEvaluator.KillEvaluatorContextStartHandler;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public sealed class TestFailMapperEvaluatorOnWaitingForEvaluatorAndExecution : TestFailMapperEvaluators
    {        
        /// <summary>
        /// This test is to fail a mapper evaluator at WaitingForEvaluator state, then fail two evaluators during the task execution
        /// </summary>
        [Fact]
        public override void TestFailedMapperOnLocalRuntime()
        {
            int chunkSize = 2;
            int dims = 10;
            int iterations = 1000;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 8;
            string testFolder = DefaultRuntimeFolder + TestId;
            TestBroadCastAndReduce(false, numTasks, chunkSize, dims, iterations, mapperMemory, updateTaskMemory, NumberOfRetry, testFolder);

            string[] lines = ReadLogFile(DriverStdout, "driver", testFolder, 240);
            var completedTaskCount = GetMessageCount(lines, CompletedTaskMessage);
            var runningTaskCount = GetMessageCount(lines, RunningTaskMessage);
            var failedEvaluatorCount = GetMessageCount(lines, FailedEvaluatorMessage);
            var failedTaskCount = GetMessageCount(lines, FailedTaskMessage);
            var jobSuccess = GetMessageCount(lines, IMRUDriver<int[], int[], int[], int[]>.DoneActionPrefix);

            // on each try each task should fail or complete or disappear with failed evaluator
            // and on each try all tasks should start successfully
            // There is an additional failed evaluator in the first time before tasks are submitted
            Assert.True((NumberOfRetry + 1) * numTasks >= completedTaskCount + failedEvaluatorCount - 1 + failedTaskCount);
            Assert.True(NumberOfRetry * numTasks < completedTaskCount + failedEvaluatorCount - 1 + failedTaskCount);
            Assert.Equal((NumberOfRetry + 1) * numTasks, runningTaskCount);

            // eventually job succeeds
            Assert.Equal(1, jobSuccess);
            CleanUp(testFolder);
        }

        /// <summary>
        /// This test is to fail a mapper evaluator at WaitingForEvaluator state, then fail two evaluators during the task execution
        /// </summary>
        [Fact]
        [Trait("Environment", "Yarn")]
        public override void TestFailedMapperOnYarn()
        {
            int chunkSize = 2;
            int dims = 10;
            int iterations = 1000;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 4;
            int numberOfRetryInRecovery = 2;
            TestBroadCastAndReduce(true, numTasks, chunkSize, dims, iterations, mapperMemory, updateTaskMemory, numberOfRetryInRecovery);
        }

        /// <summary>
        /// This method defines event handlers for driver. As default, it uses all the handlers defined in IMRUDriver.
        /// </summary>
        /// <typeparam name="TMapInput"></typeparam>
        /// <typeparam name="TMapOutput"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <typeparam name="TPartitionType"></typeparam>
        /// <returns></returns>
        protected override IConfiguration DriverEventHandlerConfigurations<TMapInput, TMapOutput, TResult, TPartitionType>()
        {
            return REEF.Driver.DriverConfiguration.ConfigurationModule
                .Set(REEF.Driver.DriverConfiguration.OnTaskCompleted, GenericType<MessageLogger>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskFailed, GenericType<MessageLogger>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskRunning, GenericType<MessageLogger>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorFailed, GenericType<MessageLogger>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorAllocated,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnDriverStarted,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnContextActive,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnContextActive,
                    GenericType<AnotherContextHandler>.Class)
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
        /// Another context handler that will submit a child context if the active context received is the specified mapper context
        /// </summary>
        internal sealed class AnotherContextHandler : IObserver<IActiveContext>
        {
            /// <summary>
            /// Make sure we only fail the context once
            /// </summary>
            private static bool done = false;

            [Inject]
            private AnotherContextHandler()
            {
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnNext(IActiveContext activeContext)
            {
                Logger.Log(Level.Info, "Receiving IActiveContext with context id {0}, Evaluator id : {1}.", activeContext.Id, activeContext.EvaluatorId);
                if (!done && activeContext.Id.StartsWith("DataLoading-Node-1"))
                {
                    Logger.Log(Level.Info, "Submitting KillEvaluatorContextStartHandler");

                   var contextConf = ContextConfiguration.ConfigurationModule
                        .Set(ContextConfiguration.Identifier, "KillEvaluatorContext")
                        .Build();

                    var childContextConf =
                        TangFactory.GetTang()
                            .NewConfigurationBuilder(contextConf)
                            .BindSetEntry<ContextConfigurationOptions.StartHandlers, KillEvaluatorContextStartHandler, IObserver<IContextStart>>(GenericType<ContextConfigurationOptions.StartHandlers>.Class, GenericType<KillEvaluatorContextStartHandler>.Class)
                            .Build();

                    activeContext.SubmitContext(childContextConf);
                    done = true;
                }
            }
        }
    }
}
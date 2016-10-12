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

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public class TestFailedUpdateEvaluatorInWaitingForEvaluatorState : IMRUBrodcastReduceTestBase
    {
        /// <summary>
        /// This test is to fail master evaluator at context start before tasks are submitted.
        /// IMRU fault tolerant driver will re-request a master evaluator when it receives IFailedEvalautor. 
        /// </summary>
        [Fact]
        public void TestFailedUpdateEvaluatorAtContextOnLocalRuntime()
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
            var failedEvaluatorCount = GetMessageCount(lines, FailedEvaluatorMessage);
            var masterContextCount = GetMessageCount(lines, "Receiving IActiveContext with context id " + IMRUConstants.MasterContextId);
            var totalContextCount = GetMessageCount(lines, "Receiving IActiveContext with context id");
            var completedTaskCount = GetMessageCount(lines, CompletedTaskMessage);
            Assert.True(1 >= failedEvaluatorCount, "failedEvaluatorCount is not expected:" + failedEvaluatorCount);
            Assert.True(numTasks + 1 == totalContextCount || numTasks == totalContextCount, "totalContextCount is not expected: " + totalContextCount); 
            Assert.True(2 >= masterContextCount, "masterContextCount is not expected:" + masterContextCount);
            Assert.Equal(numTasks, completedTaskCount);
            CleanUp(testFolder);
        }

        /// <summary>
        /// This test is for the normal scenarios of IMRUDriver and IMRUTasks on yarn
        /// </summary>
        [Fact(Skip = "Requires Yarn")]
        public void TestFailedUpdateEvaluatorAtContextOnYarn()
        {
            int chunkSize = 2;
            int dims = 10;
            int iterations = 10;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 4;
            int numberOfRetryInRecovery = 4;
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
                .Set(REEF.Driver.DriverConfiguration.OnTaskCompleted, GenericType<MessageLogger>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorFailed, GenericType<MessageLogger>.Class)
                .Set(REEF.Driver.DriverConfiguration.CustomTraceLevel, TraceLevel.Info.ToString())
                .Build();
        }

        /// <summary>
        /// Another context handler that will submit a child context if the active context received is master context
        /// and only do it once so that next time the evaluator will be successful.
        /// </summary>
        internal sealed class AnotherContextHandler : IObserver<IActiveContext>
        {
            private static bool _firstTime = true;

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
                if (activeContext.Id.Contains(IMRUConstants.MasterContextId) && _firstTime)
                {
                    var contextConf = ContextConfiguration.ConfigurationModule
                        .Set(ContextConfiguration.Identifier, "KillEvaluatorContext")
                        .Build();

                    var childContextConf =
                        TangFactory.GetTang()
                            .NewConfigurationBuilder(contextConf)
                            .BindSetEntry<ContextConfigurationOptions.StartHandlers, KillEvaluatorContextStartHandler, IObserver<IContextStart>>(GenericType<ContextConfigurationOptions.StartHandlers>.Class, GenericType<KillEvaluatorContextStartHandler>.Class)
                            .Build();

                    activeContext.SubmitContext(childContextConf);
                    _firstTime = false;
                }
            }
        }

        /// <summary>
        /// A Context start handler that is registered on a child context of master evaluator.
        /// It will kill the evaluator to simulate evaluator failure.
        /// </summary>
        internal class KillEvaluatorContextStartHandler : IObserver<IContextStart>
        {
            [Inject]
            private KillEvaluatorContextStartHandler()
            {
            }

            /// <summary>
            /// Simulate to kill the Evaluator
            /// </summary>
            /// <param name="value">context start token</param>
            public void OnNext(IContextStart value)
            {
                Environment.Exit(1);
            }

            /// <summary>
            /// Specifies what to do if error occurs. 
            /// </summary>
            /// <param name="error">Exception</param>
            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            /// <summary>
            /// Specifies what to do at completion. 
            /// </summary>
            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }
    }
}
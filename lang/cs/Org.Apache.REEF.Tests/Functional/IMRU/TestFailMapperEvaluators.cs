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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;
using TraceLevel = System.Diagnostics.TraceLevel;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public class TestFailMapperEvaluators : IMRUBrodcastReduceTestBase
    {
        private const int NumberOfRetry = 3;
        private const string CompletedTaskMessage = "CompletedTaskMessage";
        private const string RunningTaskMessage = "RunningTaskMessage";
        private const string FailedTaskMessage = "FailedTaskMessage";
        private const string FailedEvaluatorMessage = "FailedEvaluatorMessage";

        /// <summary>
        /// This test is to fail one evaluator and then try to resubmit. In the last retry, 
        /// there will be no failed evaluator and all tasks will be successfully completed. 
        /// </summary>
        [Fact]
        public void TestFailedMapperOnLocalRuntime()
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
            var completedCount = GetMessageCount(lines, CompletedTaskMessage);
            var runningTaskCount = GetMessageCount(lines, RunningTaskMessage);
            var failedEvaluatorCount = GetMessageCount(lines, FailedEvaluatorMessage);
            var failedTaskCount = GetMessageCount(lines, FailedTaskMessage);
            Assert.Equal((NumberOfRetry + 1) * numTasks, completedCount + failedEvaluatorCount + failedTaskCount);
            Assert.Equal((NumberOfRetry + 1) * numTasks, runningTaskCount);
            ////CleanUp(testFolder);
        }

        /// <summary>
        /// This test is for the normal scenarios of IMRUDriver and IMRUTasks on yarn
        /// </summary>
        [Fact(Skip = "Requires Yarn")]
        public void TestFailedMapperOnYarn()
        {
            int chunkSize = 2;
            int dims = 100;
            int iterations = 200;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 4;
            TestBroadCastAndReduce(false, numTasks, chunkSize, dims, iterations, mapperMemory, updateTaskMemory);
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
                .Set(REEF.Driver.DriverConfiguration.OnTaskCompleted,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorFailed,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnContextFailed,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskFailed,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskRunning,
                    GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskCompleted, GenericType<MessageLogger>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskFailed, GenericType<MessageLogger>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskRunning, GenericType<MessageLogger>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorFailed, GenericType<MessageLogger>.Class)
                .Set(REEF.Driver.DriverConfiguration.CustomTraceLevel, TraceLevel.Info.ToString())
                .Build();
        }

        /// <summary>
        /// Mapper function configuration. Subclass can override it to have its own test function.
        /// </summary>
        /// <returns></returns>
        protected override IConfiguration BuildMapperFunctionConfig()
        {
            return IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<TestSenderMapFunction>.Class)
                .Build();
        }

        internal sealed class TestSenderMapFunction : IMapFunction<int[], int[]>
        {
            private int _iterations;
            private readonly string _taskId;

            [Inject]
            private TestSenderMapFunction([Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId)
            {
                _taskId = taskId;
                Logger.Log(Level.Info, "TestSenderMapFunction: TaskId:" + _taskId);
            }

            /// <summary>
            /// Map function
            /// </summary>
            /// <param name="mapInput">integer array</param>
            /// <returns>The same integer array</returns>
            int[] IMapFunction<int[], int[]>.Map(int[] mapInput)
            {
                _iterations++;

                MakeException();
                Logger.Log(Level.Info, string.Format("Received value {0} in iteration {1}.", mapInput[0], _iterations));

                if (mapInput[0] != _iterations)
                {
                    Exceptions.Throw(new Exception("Expected value in mappers different from actual value"), Logger);
                }

                return mapInput;
            }

            private void MakeException()
            {
                if (_iterations == 10 && _taskId.StartsWith("IMRUMap-RandomInputPartition-2-") &&
                    !_taskId.Equals("IMRUMap-RandomInputPartition-2-" + NumberOfRetry))
                {
                    Logger.Log(Level.Warning, "Simulating Evaluator failure for taskId: " + _taskId);
                    Environment.Exit(1);
                }
            }
        }

        /// <summary>
        /// This class contains handlers for log purpose only
        /// </summary>
        sealed class MessageLogger :
            IObserver<ICompletedTask>,
            IObserver<IFailedEvaluator>,
            IObserver<IFailedTask>,
            IObserver<IRunningTask>
        {
            [Inject]
            private MessageLogger()
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

            public void OnNext(ICompletedTask value)
            {
                Logger.Log(Level.Info, CompletedTaskMessage);
            }

            public void OnNext(IFailedTask value)
            {
                Logger.Log(Level.Info, FailedTaskMessage);
            }

            public void OnNext(IFailedEvaluator value)
            {
                Logger.Log(Level.Info, FailedEvaluatorMessage);
            }

            public void OnNext(IRunningTask value)
            {
                Logger.Log(Level.Info, RunningTaskMessage);
            }
        }
    }
}
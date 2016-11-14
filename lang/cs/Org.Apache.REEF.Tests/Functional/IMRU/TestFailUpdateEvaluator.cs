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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public class TestFailUpdateEvaluator : IMRUBrodcastReduceTestBase
    {
        private const int NumberOfRetry = 3;
        protected const string FailActionMessage = "The system cannot be recovered after";

        /// <summary>
        /// This test is to fail update evaluator and then try to resubmit. We don't recover from update evaluator failure. 
        /// </summary>
        [Fact]
        public virtual void TestFailedUpdateOnLocalRuntime()
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
            var jobFailure = GetMessageCount(lines, IMRUDriver<int[], int[], int[], int[]>.FailActionPrefix);

            // there should be one try with each task either completing or disappearing with failed evaluator
            // no task failures
            // and on this try all tasks should start successfully
            Assert.Equal(numTasks, completedTaskCount + failedEvaluatorCount);
            Assert.Equal(0, failedTaskCount);
            Assert.Equal(numTasks, runningTaskCount);
            
            // job fails
            Assert.True(jobFailure > 0);
            CleanUp(testFolder);
        }

        /// <summary>
        /// This test is for the normal scenarios of IMRUDriver and IMRUTasks on yarn
        /// </summary>
        [Fact(Skip = "Requires Yarn")]
        public virtual void TestFailedUpdateOnYarn()
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
                .Set(REEF.Driver.DriverConfiguration.CustomTraceLevel, TraceLevel.Verbose.ToString())
                .Build();
        }

        /// <summary>
        /// Mapper function configuration. Subclass can override it to have its own test function.
        /// </summary>
        /// <returns></returns>
        protected override IConfiguration BuildUpdateFunctionConfigModule()
        {
            var c = IMRUUpdateConfiguration<int[], int[], int[]>.ConfigurationModule
                .Set(IMRUUpdateConfiguration<int[], int[], int[]>.UpdateFunction,
                    GenericType<TestUpdateFunction>.Class)                   
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(c)
                .BindIntNamedParam<PipelinedBroadcastAndReduceWithFaultTolerant.FailureType>(PipelinedBroadcastAndReduceWithFaultTolerant.FailureType.EvaluatorFailureDuringTaskExecution.ToString())
                .Build();
        }

        internal sealed class TestUpdateFunction : IUpdateFunction<int[], int[], int[]>
        {
            private int _iterations;
            private readonly int _maxIters;
            private readonly int _dim;
            private readonly int[] _intArr;
            private readonly int _workers;
            private readonly string _taskId;
            private int _failureType;

            [Inject]
            private TestUpdateFunction(
                [Parameter(typeof(BroadcastReduceConfiguration.NumberOfIterations))] int maxIters,
                [Parameter(typeof(BroadcastReduceConfiguration.Dimensions))] int dim,
                [Parameter(typeof(BroadcastReduceConfiguration.NumWorkers))] int numWorkers,
                [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
                [Parameter(typeof(PipelinedBroadcastAndReduceWithFaultTolerant.FailureType))] int failureType)
            {
                _maxIters = maxIters;
                _iterations = 0;
                _dim = dim;
                _intArr = new int[_dim];
                _workers = numWorkers;
                _taskId = taskId;
                _failureType = failureType;
                Logger.Log(Level.Info, "TestUpdateFunction: TaskId: {0}", _taskId);
                Logger.Log(Level.Info, "Failure type: {0} failure", PipelinedBroadcastAndReduceWithFaultTolerant.FailureType.IsEvaluatorFailure(_failureType) ? "evaluator" : "task");
            }

            /// <summary>
            /// Update function
            /// </summary>
            /// <param name="input">integer array</param>
            /// <returns>The same integer array</returns>
            UpdateResult<int[], int[]> IUpdateFunction<int[], int[], int[]>.Update(int[] input)
            {
                if (input[0] != (_iterations + 1) * _workers)
                {
                    Exceptions.Throw(new Exception("Expected input to update functon not same as actual input"), Logger);
                }

                _iterations++;
                Logger.Log(Level.Info, "Received value {0} in iteration {1}", input[0], _iterations);
                MakeException();

                if (_iterations < _maxIters)
                {
                    for (int i = 0; i < _dim; i++)
                    {
                        _intArr[i] = _iterations + 1;
                    }

                    return UpdateResult<int[], int[]>.AnotherRound(_intArr);
                }

                return UpdateResult<int[], int[]>.Done(input);
            }

            /// <summary>
            /// Initialize function. Sends integer array with value 1 to all mappers
            /// </summary>
            /// <returns>Map input</returns>
            UpdateResult<int[], int[]> IUpdateFunction<int[], int[], int[]>.Initialize()
            {
                for (int i = 0; i < _dim; i++)
                {
                    _intArr[i] = _iterations + 1;
                }

                return UpdateResult<int[], int[]>.AnotherRound(_intArr);
            }

            private void MakeException()
            {
                if (_iterations == 10 && !_taskId.EndsWith("-" + NumberOfRetry))
                { 
                    Logger.Log(Level.Warning, "Simulating {0} failure for taskId {1}",
                        PipelinedBroadcastAndReduceWithFaultTolerant.FailureType.IsEvaluatorFailure(_failureType) ? "evaluator" : "task",
                        _taskId);
                    if (PipelinedBroadcastAndReduceWithFaultTolerant.FailureType.IsEvaluatorFailure(_failureType))
                    {
                        // simulate evaluator failure
                        Environment.Exit(1);
                    }
                    else
                    {
                        // simulate task failure
                        throw new ArgumentNullException("Simulating task failure");
                    }
                }
            }
        }
    }
}
﻿// Licensed to the Apache Software Foundation (ASF) under one
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
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.Network;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
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
        protected const int NumberOfRetry = 3;

        /// <summary>
        /// This test is to fail one evaluator and then try to resubmit. In the last retry, 
        /// there will be no failed evaluator and all tasks will be successfully completed. 
        /// </summary>
        [Fact]
        public virtual void TestFailedMapperOnLocalRuntime()
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
            var completedTaskCount = GetMessageCount(lines, CompletedTaskMessage);
            var runningTaskCount = GetMessageCount(lines, RunningTaskMessage);
            var failedEvaluatorCount = GetMessageCount(lines, FailedEvaluatorMessage);
            var failedTaskCount = GetMessageCount(lines, FailedTaskMessage);
            Assert.Equal((NumberOfRetry + 1) * numTasks, completedTaskCount + failedEvaluatorCount + failedTaskCount);
            Assert.Equal((NumberOfRetry + 1) * numTasks, runningTaskCount);
            CleanUp(testFolder);
        }

        /// <summary>
        /// This test is for the normal scenarios of IMRUDriver and IMRUTasks on yarn
        /// </summary>
        [Fact(Skip = "Requires Yarn")]
        public virtual void TestFailedMapperOnYarn()
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
        protected override IConfiguration BuildMapperFunctionConfig()
        {
            var c1 = IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<TestSenderMapFunction>.Class)                   
                .Build();

            var c2 = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<TaskIdsToFail, string>(GenericType<TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-2-")
                .BindSetEntry<TaskIdsToFail, string>(GenericType<TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-3-")
                .BindIntNamedParam<FailureType>("0")
                .Build();

            return Configurations.Merge(c1, c2, GetTcpConfiguration());
        }

        /// <summary>
        /// Update function configuration. Subclass can override it to have its own test function.
        /// </summary>
        /// <returns></returns>
        protected override IConfiguration BuildUpdateFunctionConfig()
        {
            var c = IMRUUpdateConfiguration<int[], int[], int[]>.ConfigurationModule
                .Set(IMRUUpdateConfiguration<int[], int[], int[]>.UpdateFunction,
                    GenericType<BroadcastSenderReduceReceiverUpdateFunction>.Class)
                .Build();

            return Configurations.Merge(c, GetTcpConfiguration());
        }

        /// <summary>
        /// Override default setting for retry policy
        /// </summary>
        /// <returns></returns>
        private IConfiguration GetTcpConfiguration()
        {
            return TcpClientConfigurationModule.ConfigurationModule
                .Set(TcpClientConfigurationModule.MaxConnectionRetry, "200")
                .Set(TcpClientConfigurationModule.SleepTime, "1000")
                .Build();
        }

        [NamedParameter]
        protected class TaskIdsToFail : Name<ISet<string>>
        {            
        }

        [NamedParameter(Documentation = "Type of failure to simulate: 0/1 - eval/task during task execution, 2/3 - during task initialization")]
        protected class FailureType : Name<int>
        {
        }

        internal sealed class TestSenderMapFunction : IMapFunction<int[], int[]>
        {
            private int _iterations;
            private readonly string _taskId;
            private readonly ISet<string> _taskIdsToFail;
            private int _failureType;

            [Inject]
            private TestSenderMapFunction(
                [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
                [Parameter(typeof(TaskIdsToFail))] ISet<string> taskIdsToFail,
                [Parameter(typeof(FailureType))] int failureType)
            {
                _taskId = taskId;
                _taskIdsToFail = taskIdsToFail;
                _failureType = failureType;
                Logger.Log(Level.Info, "TestSenderMapFunction: TaskId: {0}", _taskId);
                foreach (var n in _taskIdsToFail)
                {
                    Logger.Log(Level.Info, "TestSenderMapFunction: taskIdsToFail: {0}", n);
                }
                Logger.Log(Level.Info, "Failure type: {0}", _failureType);

                if (_failureType == 2 || _failureType == 3)
                {
                    SimulateFailure(0);
                }
            }

            /// <summary>
            /// Map function
            /// </summary>
            /// <param name="mapInput">integer array</param>
            /// <returns>The same integer array</returns>
            int[] IMapFunction<int[], int[]>.Map(int[] mapInput)
            {
                _iterations++;
                Logger.Log(Level.Info, "Received value {0} in iteration {1}.", mapInput[0], _iterations);

                if (_failureType <= 1)
                {
                    SimulateFailure(10);
                }

                if (mapInput[0] != _iterations)
                {
                    Exceptions.Throw(new Exception("Expected value in mappers different from actual value"), Logger);
                }

                return mapInput;
            }

            private void SimulateFailure(int onIteration)
            {
                if (_iterations == onIteration && 
                    _taskIdsToFail.FirstOrDefault(e => _taskId.StartsWith(e)) != null &&
                    _taskIdsToFail.FirstOrDefault(e => _taskId.Equals(e + NumberOfRetry)) == null)
                { 
                    Logger.Log(Level.Warning, "Simulating {0} failure for taskId {1}",
                        _failureType % 2 == 0 ? "evaluator" : "task",
                        _taskId);
                    if (_failureType % 2 == 0)
                    {
                        // simulate evaluator failure
                        Environment.Exit(1);
                    }
                    else
                    {
                        // simulate task failure
                        throw new ArgumentNullException();
                    }
                }
            }
        }
    }
}
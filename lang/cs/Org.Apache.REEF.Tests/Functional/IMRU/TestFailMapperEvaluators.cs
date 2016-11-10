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

using System.Diagnostics;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Network;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public class TestFailMapperEvaluators : IMRUBrodcastReduceTestBase
    {
        protected const int NumberOfRetry = 3;

        /// <summary>
        /// This test fails two evaluators during task execution stage on each retry except last.
        /// Job is retried until success. 
        /// In each retry, when the task is restarted on an existing evaluator, the iteration will continue from the previous task state.
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
            var jobSuccess = GetMessageCount(lines, IMRUDriver<int[], int[], int[], int[]>.DoneActionPrefix);

            // on each try each task should fail or complete or disappear with failed evaluator
            // and on each try all tasks should start successfully
            Assert.Equal((NumberOfRetry + 1) * numTasks, completedTaskCount + failedEvaluatorCount + failedTaskCount);
            Assert.Equal((NumberOfRetry + 1) * numTasks, runningTaskCount);
            // eventually job succeeds
            Assert.Equal(1, jobSuccess);
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
        /// Create IMRU Job Definition with IMRU required configurations
        /// </summary>
        /// <param name="numberofMappers"></param>
        /// <param name="chunkSize"></param>
        /// <param name="numIterations"></param>
        /// <param name="dim"></param>
        /// <param name="mapperMemory"></param>
        /// <param name="updateTaskMemory"></param>
        /// <param name="numberOfRetryInRecovery"></param>
        /// <param name="numberOfChecksBeforeCancellingJob"></param>
        /// <returns></returns>
        protected override IMRUJobDefinition CreateIMRUJobDefinitionBuilder(int numberofMappers,
            int chunkSize,
            int numIterations,
            int dim,
            int mapperMemory,
            int updateTaskMemory,
            int numberOfRetryInRecovery,
            int? numberOfChecksBeforeCancellingJob = null)
        {
            return new IMRUJobDefinitionBuilder()
                .SetUpdateTaskStateConfiguration(UpdateTaskStateConfiguration())
                .SetMapTaskStateConfiguration(MapTaskStateConfiguration())
                .SetMapFunctionConfiguration(BuildMapperFunctionConfig())
                .SetUpdateFunctionConfiguration(BuildUpdateFunctionConfiguration(numberofMappers, numIterations, dim))
                .SetMapInputCodecConfiguration(BuildMapInputCodecConfig())
                .SetUpdateFunctionCodecsConfiguration(BuildUpdateFunctionCodecsConfig())
                .SetReduceFunctionConfiguration(BuildReduceFunctionConfig())
                .SetMapInputPipelineDataConverterConfiguration(BuildDataConverterConfig(chunkSize))
                .SetMapOutputPipelineDataConverterConfiguration(BuildDataConverterConfig(chunkSize))
                .SetPartitionedDatasetConfiguration(BuildPartitionedDatasetConfiguration(numberofMappers))
                .SetJobName(IMRUJobName)
                .SetNumberOfMappers(numberofMappers)
                .SetMapperMemory(mapperMemory)
                .SetUpdateTaskMemory(updateTaskMemory)
                .SetMaxRetryNumberInRecovery(numberOfRetryInRecovery)
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
                    GenericType<PipelinedBroadcastAndReduceWithFaultTolerant.SenderMapFunctionFT>.Class)
                .Build();

            var c2 = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<PipelinedBroadcastAndReduceWithFaultTolerant.TaskIdsToFail, string>(GenericType<PipelinedBroadcastAndReduceWithFaultTolerant.TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-2-")
                .BindSetEntry<PipelinedBroadcastAndReduceWithFaultTolerant.TaskIdsToFail, string>(GenericType<PipelinedBroadcastAndReduceWithFaultTolerant.TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-3-")
                .BindIntNamedParam<PipelinedBroadcastAndReduceWithFaultTolerant.FailureType>(PipelinedBroadcastAndReduceWithFaultTolerant.FailureType.EvaluatorFailureDuringTaskExecution.ToString())
                .BindNamedParameter(typeof(MaxRetryNumberInRecovery), NumberOfRetry.ToString())
                .BindNamedParameter(typeof(PipelinedBroadcastAndReduceWithFaultTolerant.TotalNumberOfForcedFailures), NumberOfRetry.ToString())
                .Build();

            return Configurations.Merge(c1, c2, GetTcpConfiguration());
        }

        /// <summary>
        /// Set update function to IMRUUpdateConfiguration configuration module. Return it with TCP configuration.
        /// </summary>
        /// <returns></returns>
        protected override IConfiguration BuildUpdateFunctionConfigModule()
        {
            var c = IMRUUpdateConfiguration<int[], int[], int[]>.ConfigurationModule
                .Set(IMRUUpdateConfiguration<int[], int[], int[]>.UpdateFunction,
                    GenericType<PipelinedBroadcastAndReduceWithFaultTolerant.BroadcastSenderReduceReceiverUpdateFunctionFT>.Class)
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

        /// <summary>
        /// Configuration for Map task state
        /// </summary>
        /// <returns></returns>
        private IConfiguration MapTaskStateConfiguration()
        {
            return TangFactory.GetTang()
                   .NewConfigurationBuilder()
                   .BindImplementation(GenericType<ITaskState>.Class, GenericType<MapTaskState<int[]>>.Class)
                   .Build();
        }

        /// <summary>
        /// Configuration for Update task state
        /// </summary>
        /// <returns></returns>
        private IConfiguration UpdateTaskStateConfiguration()
        {
            return TangFactory.GetTang()
                   .NewConfigurationBuilder()
                   .BindImplementation(GenericType<ITaskState>.Class, GenericType<UpdateTaskState<int[], int[]>>.Class)
                   .Build();
        }
    }
}
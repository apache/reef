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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using System.Threading;
using Org.Apache.REEF.Common.Poison;

namespace Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce
{
    /// <summary>
    /// IMRU program that performs broadcast and reduce with fault tolerance.
    /// </summary>
    public sealed class PipelinedBroadcastAndReduceWithRandomFailures : PipelinedBroadcastAndReduce
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PipelinedBroadcastAndReduceWithRandomFailures));

        [Inject]
        private PipelinedBroadcastAndReduceWithRandomFailures(IIMRUClient imruClient) : base(imruClient)
        {
        }

        /// <summary>
        /// Runs the actual broadcast and reduce job with fault tolerance
        /// </summary>
        internal void Run(int numberofMappers, int chunkSize, int numIterations, int dim, int mapperMemory, int updateTaskMemory, 
            int maxRetryNumberInRecovery, float failProb, int minTimeoutInSec, int expThroughput)
        {
            var results = _imruClient.Submit<int[], int[], int[], Stream>(
                CreateJobDefinitionBuilder(numberofMappers, chunkSize, numIterations, dim, mapperMemory, updateTaskMemory, failProb, minTimeoutInSec)
                    .SetMapFunctionConfiguration(BuildMapperFunctionConfig(dim, failProb, minTimeoutInSec, expThroughput))
                    .SetMaxRetryNumberInRecovery(maxRetryNumberInRecovery)
                    .Build());
        }

        private IMRUJobDefinitionBuilder CreateJobDefinitionBuilder(int numberofMappers, int chunkSize, int numIterations, int dim, int mapperMemory, int updateTaskMemory,
            float failProb, int minTimeoutInSec)
        {
            return new IMRUJobDefinitionBuilder()
                    .SetUpdateTaskStateConfiguration(UpdateTaskStateConfiguration())
                    .SetMapTaskStateConfiguration(MapTaskStateConfiguration())
                    .SetUpdateFunctionConfiguration(UpdateFunctionConfig(numberofMappers, numIterations, dim))
                    .SetMapInputCodecConfiguration(MapInputCodecConfiguration())
                    .SetUpdateFunctionCodecsConfiguration(UpdateFunctionCodecsConfiguration())
                    .SetReduceFunctionConfiguration(ReduceFunctionConfiguration())
                    .SetMapInputPipelineDataConverterConfiguration(MapInputDataConverterConfig(chunkSize))
                    .SetMapOutputPipelineDataConverterConfiguration(MapOutputDataConverterConfig(chunkSize))
                    .SetPartitionedDatasetConfiguration(PartitionedDatasetConfiguration(numberofMappers))
                    .SetJobName(string.Format("BroadcastReduceWithRandomFailures_failProb={0}_minTimeout={1}sec", failProb.ToString().Replace("0.", "pt"), minTimeoutInSec))
                    .SetNumberOfMappers(numberofMappers)
                    .SetMapperMemory(mapperMemory)
                    .SetUpdateTaskMemory(updateTaskMemory);
        }

        /// <summary>
        /// Build a test mapper function configuration
        /// </summary>
        /// <param name="dim">Dimensionality of array being communicated</param>
        /// <param name="failProb">Probability of failure</param>
        /// <param name="minTimeoutInSec">Minimum timeout before which induced failure does not occur</param>
        /// <param name="expThroughput">Throughput to be expected from IMRU. Used to compute max timeout</param>
        private IConfiguration BuildMapperFunctionConfig(int dim, float failProb, int minTimeoutInSec, int expThroughputInMBps)
        {
            var c1 = IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<SenderMapFunctionFT>.Class)
                .Build();

            var minTimeOut = minTimeoutInSec * 1000;
            var expThroughput = expThroughputInMBps * 1000000;
            var maxTimeoutInMilliSec = Math.Max(minTimeOut, (int)Math.Ceiling(dim * sizeof(int) * 1000.0 / expThroughput));

            var c2 = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter(typeof(CrashProbability), failProb.ToString())
                .BindIntNamedParam<CrashMinDelay>((minTimeoutInSec * 1000).ToString())
                .Build();

            return Configurations.Merge(c1, c2);
        }

        /// <summary>
        /// Configuration for Update Function
        /// </summary>
        protected override IConfiguration BuildUpdateFunctionConfig()
        {
            return IMRUUpdateConfiguration<int[], int[], int[]>.ConfigurationModule
                .Set(IMRUUpdateConfiguration<int[], int[], int[]>.UpdateFunction,
                    GenericType<PipelinedBroadcastAndReduceWithFaultTolerant.BroadcastSenderReduceReceiverUpdateFunctionFT>.Class).Build();
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

        /// <summary>
        /// Configuration for map task state
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
        /// The function is to simulate Evaluator/Task failure for mapper evaluator
        /// </summary>
        internal sealed class SenderMapFunctionFT : IMapFunction<int[], int[]>, IDisposable
        {
            private int _iterations;
            private readonly string _taskId;
            private readonly MapTaskState<int[]> _taskState;
            private readonly int _retryIndex;

            [Inject]
            private SenderMapFunctionFT(
                [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
                PoisonedEventHandler<SenderMapFunctionFT> poisonedHandler,
                ITaskState taskState)
            {
                _taskId = taskId;
                poisonedHandler.OnNext(this);
                var chosenToFail = poisonedHandler.TimeToCrash >= 0;
                _taskState = (MapTaskState<int[]>)taskState;

                var taskIdSplit = taskId.Split('-');
                _retryIndex = int.Parse(taskIdSplit[taskIdSplit.Length - 1]);

                if (chosenToFail)
                {
                    Logger.Log(Level.Info,
                        "SenderMapFunctionFT.Ctor: TaskId: {0}, RetryNumber: {1}, Time To Crash: {2:F2} sec.",
                        _taskId,
                        _retryIndex,
                        poisonedHandler.TimeToCrash / 1000.0);
                }
                else
                {
                    Logger.Log(Level.Info,
                        "SenderMapFunctionFT.Ctor: TaskId: {0}, RetryNumber: {1} *****Not Chosen for Failure in this retry attempt*****",
                        _taskId,
                        _retryIndex);
                }
            }

            /// <summary>
            /// Map function
            /// It simply takes the input, does a verification for the task state and returns the same input value. 
            /// </summary>
            /// <param name="mapInput">integer array</param>
            /// <returns>The same integer array</returns>
            int[] IMapFunction<int[], int[]>.Map(int[] mapInput)
            {
                int[] previousValue = _taskState.CurrentValue;

                // In this example, when task is re-started, with the task state management, it should continue from the previous number
                if (previousValue != null && previousValue[0] > mapInput[0])
                {
                    throw new Exception(string.Format(CultureInfo.CurrentCulture,
                        "The previous value received was {0} but the value received is {1} which is smaller than the previous value.",
                        previousValue[0],
                        mapInput[0]));
                }

                _taskState.CurrentValue = mapInput;

                _iterations++;
                Logger.Log(Level.Info, "SenderMapFunctionFT.Map: TaskId: {0} Received value {1} in iteration {2}.", _taskId, mapInput[0], _iterations);

                return mapInput;
            }

            public void Dispose()
            {
            }
        }
    }
}
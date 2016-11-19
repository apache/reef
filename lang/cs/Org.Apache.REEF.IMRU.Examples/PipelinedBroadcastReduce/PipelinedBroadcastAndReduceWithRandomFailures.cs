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
                CreateJobDefinitionBuilder(numberofMappers, chunkSize, numIterations, dim, mapperMemory, updateTaskMemory)
                    .SetMapFunctionConfiguration(BuildMapperFunctionConfig(dim, failProb, minTimeoutInSec, expThroughput))
                    .SetMaxRetryNumberInRecovery(maxRetryNumberInRecovery)
                    .Build());
        }

        protected override IMRUJobDefinitionBuilder CreateJobDefinitionBuilder(int numberofMappers, int chunkSize, int numIterations, int dim, int mapperMemory, int updateTaskMemory)
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
                    .SetJobName("BroadcastReduce")
                    .SetNumberOfMappers(numberofMappers)
                    .SetMapperMemory(mapperMemory)
                    .SetUpdateTaskMemory(updateTaskMemory);
        }

        /// <summary>
        /// Build a test mapper function configuration
        /// </summary>
        /// <param name="maxRetryInRecovery">Number of retries done if first run failed.</param>
        /// <param name="totalNumberOfForcedFailures">Number of forced failure times in recovery.</param>
        private IConfiguration BuildMapperFunctionConfig(int dim, float failProb, int minTimeoutInSec, int expThroughput)
        {
            var c1 = IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<SenderMapFunctionFT>.Class)
                .Build();

            var c2 = TangFactory.GetTang().NewConfigurationBuilder()
                .BindIntNamedParam<Dimensions>(dim.ToString())
                .BindNamedParameter(typeof(FailureProbability), failProb.ToString())
                .BindIntNamedParam<MinTimeoutInSec>(minTimeoutInSec.ToString())
                .BindIntNamedParam<ExpectedThroughput>(expThroughput.ToString())
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
                    GenericType<BroadcastSenderReduceReceiverUpdateFunctionFT>.Class).Build();
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

        [NamedParameter(Documentation = "The dimensions", DefaultValue = "10")]
        internal class Dimensions : Name<int>
        {
        }

        [NamedParameter(Documentation = "Failure Probability", DefaultValue = "0.1")]
        internal class FailureProbability : Name<float>
        {
        }

        [NamedParameter(Documentation = "Expected Throughput in MBps. Used to compute the max timeout using 4*dimensions / ExpThroughput*1000000", DefaultValue = "1" /*MBps*/)]
        internal class ExpectedThroughput : Name<int>
        {
        }

        [NamedParameter(Documentation = "Minimum Timeout in seconds. Failure will not happen before this.", DefaultValue = "30")]
        internal class MinTimeoutInSec : Name<int>
        {
        }

        internal sealed class Exit
        {
            private string _taskId;
            private int _timeInMilliSec;

            public Exit(string taskId, int timeInMilliSec)
            {
                _taskId = taskId;
                _timeInMilliSec = timeInMilliSec;
                Logger.Log(Level.Info, "ExitController: TaskId: {0} Setting to die in {1} sec", _taskId, _timeInMilliSec / 1000.0);
            }

            public void After()
            {
                Thread.Sleep(_timeInMilliSec);
                Logger.Log(Level.Warning, "ExitController: TaskId: {0} exiting now", _taskId);
                Environment.Exit(1);
            }
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
                [Parameter(typeof(Dimensions))] int dim,
                [Parameter(typeof(FailureProbability))] float failProb,
                [Parameter(typeof(MinTimeoutInSec))] int minTimeoutInSec,
                [Parameter(typeof(ExpectedThroughput))] int expThroughputInMBps,
                ITaskState taskState)
            {
                _taskId = taskId;
                var rnd = new Random();
                var toss = rnd.NextDouble();
                var maxTimeout = 0.0;
                var chosenToFail = false;
                if (toss < failProb)
                {
                    chosenToFail = true;
                    var minTimeOut = minTimeoutInSec * 1000;
                    var expThroughput = expThroughputInMBps * 1000000;
                    var maxTimeoutInMilliSec = Math.Max(minTimeOut, (int)Math.Ceiling(dim * 4 * 1000.0 / expThroughput));
                    var exit = new Exit(_taskId, minTimeOut + rnd.Next(maxTimeoutInMilliSec));
                    var thread = new Thread(exit.After);
                    maxTimeout = minTimeoutInSec + (maxTimeoutInMilliSec / 1000.0);
                    thread.Start();
                }
                _taskState = (MapTaskState<int[]>)taskState;

                var taskIdSplit = taskId.Split('-');
                _retryIndex = int.Parse(taskIdSplit[taskIdSplit.Length - 1]);

                if (chosenToFail)
                {
                    Logger.Log(Level.Info,
                        "SenderMapFunctionFT.Ctor: TaskId: {0}, RetryNumber: {1}, Dimensions: {2}, Failure Prob: {3}, MinTimeoutInSec: {4}, MaxTimeoutInSec: {5:F2}.",
                        _taskId,
                        _retryIndex,
                        dim,
                        failProb,
                        minTimeoutInSec,
                        maxTimeout);
                }
                else
                {
                    Logger.Log(Level.Info,
                        "SenderMapFunctionFT.Ctor: TaskId: {0}, RetryNumber: {1}, Dimensions: {2}, *****Not Chosen for Failure in this retry attempt*****",
                        _taskId,
                        _retryIndex,
                        dim);
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

        /// <summary>
        /// The Update function for integer array broadcast and reduce
        /// </summary>
        internal sealed class BroadcastSenderReduceReceiverUpdateFunctionFT : IUpdateFunction<int[], int[], int[]>
        {
            private int _iterations;
            private readonly int _maxIters;
            private readonly int _dim;
            private readonly int[] _intArr;
            private readonly int _workers;
            private readonly UpdateTaskState<int[], int[]> _taskState;

            [Inject]
            private BroadcastSenderReduceReceiverUpdateFunctionFT(
                [Parameter(typeof(BroadcastReduceConfiguration.NumberOfIterations))] int maxIters,
                [Parameter(typeof(BroadcastReduceConfiguration.Dimensions))] int dim,
                [Parameter(typeof(BroadcastReduceConfiguration.NumWorkers))] int numWorkers,
                ITaskState taskState)
            {
                _maxIters = maxIters;
                _iterations = 0;
                _dim = dim;
                _intArr = new int[_dim];
                _workers = numWorkers;
                _taskState = (UpdateTaskState<int[], int[]>)taskState;
            }

            /// <summary>
            /// Update function
            /// </summary>
            /// <param name="input">Input containing sum of all mappers arrays</param>
            /// <returns>The Update Result</returns>
            UpdateResult<int[], int[]> IUpdateFunction<int[], int[], int[]>.Update(int[] input)
            {
                Logger.Log(Level.Info, "Received value {0} in iterations {1}.", input[0], _iterations + 1);

                if (input[0] != (_iterations + 1) * _workers)
                {
                    throw new Exception("Expected input to update function not same as actual input");
                }

                _iterations++;

                if (_iterations < _maxIters)
                {
                    for (int i = 0; i < _dim; i++)
                    {
                        _intArr[i] = _iterations + 1;
                    }

                    SaveState(_intArr);
                    return UpdateResult<int[], int[]>.AnotherRound(_intArr);
                }
                SaveResult(input);
                return UpdateResult<int[], int[]>.Done(input);
            }

            /// <summary>
            /// Initialize function. Sends integer array with value 1 to all mappers first time.
            /// In recovery case, restore the state and iterations from task state.
            /// </summary>
            /// <returns>Map input</returns>
            UpdateResult<int[], int[]> IUpdateFunction<int[], int[], int[]>.Initialize()
            {
                if (_taskState.Result != null)
                {
                    Restore(_taskState.Result);
                    _iterations = _taskState.Iterations;
                    return UpdateResult<int[], int[]>.Done(_intArr);
                }

                if (_taskState.Input != null)
                {
                    Restore(_taskState.Input);
                    _iterations = _taskState.Iterations;
                    return UpdateResult<int[], int[]>.AnotherRound(_intArr);
                }

                for (int i = 0; i < _dim; i++)
                {
                    _intArr[i] = 1;
                }
                return UpdateResult<int[], int[]>.AnotherRound(_intArr);
            }

            /// <summary>
            /// Save the current value to Task State
            /// </summary>
            /// <param name="value"></param>
            private void SaveState(int[] value)
            {
                _taskState.Iterations = _iterations;
                _taskState.Input = value;
                Logger.Log(Level.Info, "State saved: {0}", _taskState.Input[0]);
            }

            /// <summary>
            /// Save the result to Task State
            /// </summary>
            /// <param name="value"></param>
            private void SaveResult(int[] value)
            {
                _taskState.Iterations = _iterations;
                _taskState.Result = value;
                Logger.Log(Level.Info, "Result saved: {0}", _taskState.Result[0]);
            }

            /// <summary>
            /// Restore the data back to _intArr
            /// </summary>
            /// <param name="d"></param>
            private void Restore(int[] d)
            {
                for (int i = 0; i < _dim; i++)
                {
                    _intArr[i] = d[i];
                }
            }
        }
    }
}
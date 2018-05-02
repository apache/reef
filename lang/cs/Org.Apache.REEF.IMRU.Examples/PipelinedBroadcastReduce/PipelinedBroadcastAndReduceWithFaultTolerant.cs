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
using Newtonsoft.Json;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.CheckpointHandler;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.IO.TempFileCreation;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce
{
    /// <summary>
    /// IMRU program that performs broadcast and reduce with fault tolerance.
    /// </summary>
    internal sealed class PipelinedBroadcastAndReduceWithFaultTolerant : PipelinedBroadcastAndReduce
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PipelinedBroadcastAndReduceWithFaultTolerant));

        [Inject]
        private PipelinedBroadcastAndReduceWithFaultTolerant(IIMRUClient imruClient) : base(imruClient)
        {
        }

        /// <summary>
        /// Runs the actual broadcast and reduce job with fault tolerance
        /// </summary>
        public void Run(int numberofMappers, int chunkSize, int numIterations, int dim, int mapperMemory, int updateTaskMemory, int maxRetryNumberInRecovery, int totalNumberOfForcedFailures)
        {
            var results = _imruClient.Submit<int[], int[], int[], Stream>(
                CreateJobDefinitionBuilder(numberofMappers, chunkSize, numIterations, dim, mapperMemory, updateTaskMemory)
                    .SetMapFunctionConfiguration(BuildMapperFunctionConfig(maxRetryNumberInRecovery, totalNumberOfForcedFailures))
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
                    .SetResultHandlerConfiguration(BuildResultHandlerConfig())
                    .SetCheckpointConfiguration(BuildCheckpointConfig())
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
        private IConfiguration BuildMapperFunctionConfig(int maxRetryInRecovery, int totalNumberOfForcedFailures)
        {
            var c1 = IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<SenderMapFunctionFT>.Class)
                .Build();

            var c2 = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<TaskIdsToFail, string>(GenericType<TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-2-")
                .BindSetEntry<TaskIdsToFail, string>(GenericType<TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-3-")
                .BindIntNamedParam<FailureType>(FailureType.EvaluatorFailureDuringTaskExecution.ToString())
                .BindNamedParameter(typeof(MaxRetryNumberInRecovery), maxRetryInRecovery.ToString())
                .BindNamedParameter(typeof(TotalNumberOfForcedFailures), totalNumberOfForcedFailures.ToString())
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
        /// Build checkpoint configuration. Subclass can override it.
        /// </summary>
        protected override IConfiguration BuildCheckpointConfig()
        {
            var filePath = TangFactory.GetTang().NewInjector().GetInstance<ITempFileCreator>().CreateTempDirectory("statefiles", string.Empty);

            return CheckpointConfigurationBuilder.ConfigurationModule
                .Set(CheckpointConfigurationBuilder.CheckpointFilePath, filePath)
                .Set(CheckpointConfigurationBuilder.TaskStateCodec, GenericType<UpdateTaskStateCodec>.Class)
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

        [NamedParameter(Documentation = "Set of task ids which will produce task/evaluator failure")]
        internal class TaskIdsToFail : Name<ISet<string>>
        {
        }

        [NamedParameter(Documentation = "Type of failure to simulate")]
        internal class FailureType : Name<int>
        {
            internal const int EvaluatorFailureDuringTaskExecution = 0;
            internal const int TaskFailureDuringTaskExecution = 1;
            internal const int EvaluatorFailureDuringTaskInitialization = 2;
            internal const int TaskFailureDuringTaskInitialization = 3;
            internal const int EvaluatorFailureDuringTaskDispose = 4;
            internal const int TaskFailureDuringTaskDispose = 5;

            internal static bool IsEvaluatorFailure(int failureType)
            {
                return failureType == EvaluatorFailureDuringTaskExecution ||
                       failureType == EvaluatorFailureDuringTaskInitialization ||
                       failureType == EvaluatorFailureDuringTaskDispose;
            }
        }

        [NamedParameter(Documentation = "Total number of failures in recovery.", DefaultValue = "2")]
        internal class TotalNumberOfForcedFailures : Name<int>
        {
        }

        /// <summary>
        /// The function is to simulate Evaluator/Task failure for mapper evaluator
        /// </summary>
        internal sealed class SenderMapFunctionFT : IMapFunction<int[], int[]>, IDisposable
        {
            private int _iterations;
            private readonly string _taskId;
            private readonly MapTaskState<int[]> _taskState;
            private readonly ISet<string> _taskIdsToFail;
            private readonly int _failureType;
            private readonly int _maxRetryInRecovery;
            private readonly int _totalNumberOfForcedFailures;
            private readonly int _retryIndex;

            [Inject]
            private SenderMapFunctionFT(
                [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
                [Parameter(typeof(TaskIdsToFail))] ISet<string> taskIdsToFail,
                [Parameter(typeof(FailureType))] int failureType,
                [Parameter(typeof(MaxRetryNumberInRecovery))] int maxRetryNumberInRecovery,
                [Parameter(typeof(TotalNumberOfForcedFailures))] int totalNumberOfForcedFailures,                
                ITaskState taskState)
            {
                _taskId = taskId;
                _taskState = (MapTaskState<int[]>)taskState;
                _taskIdsToFail = taskIdsToFail;
                _failureType = failureType;
                _maxRetryInRecovery = maxRetryNumberInRecovery;
                _totalNumberOfForcedFailures = totalNumberOfForcedFailures;

                var taskIdSplit = taskId.Split('-');
                _retryIndex = int.Parse(taskIdSplit[taskIdSplit.Length - 1]);

                Logger.Log(Level.Info,
                    "TestSenderMapFunction: TaskId: {0}, _maxRetryInRecovery {1}, totalNumberOfForcedFailures: {2}, RetryNumber: {3}, Failure type: {4}.",
                    _taskId,
                    _maxRetryInRecovery,
                    _totalNumberOfForcedFailures,
                    _retryIndex,
                    _failureType);
                foreach (var n in _taskIdsToFail)
                {
                    Logger.Log(Level.Info, "TestSenderMapFunction: taskIdsToFail: {0}", n);
                }

                if (_failureType == FailureType.EvaluatorFailureDuringTaskInitialization ||
                    _failureType == FailureType.TaskFailureDuringTaskInitialization)
                {
                    SimulateFailure(0);
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
                Logger.Log(Level.Info, "Received value {0} in iteration {1}.", mapInput[0], _iterations);

                if (_failureType == FailureType.EvaluatorFailureDuringTaskExecution ||
                    _failureType == FailureType.TaskFailureDuringTaskExecution)
                {
                    SimulateFailure(10);
                }

                return mapInput;
            }

            public void Dispose()
            {
                if (_failureType == FailureType.EvaluatorFailureDuringTaskDispose ||
                    _failureType == FailureType.TaskFailureDuringTaskDispose)
                {
                    SimulateFailure(_iterations);
                }
            }

            private void SimulateFailure(int onIteration)
            {
                if (_iterations == onIteration &&
                    _taskIdsToFail.FirstOrDefault(e => _taskId.StartsWith(e)) != null &&
                    _taskIdsToFail.FirstOrDefault(e => _taskId.Equals(e + _maxRetryInRecovery)) == null &&
                    _retryIndex < _totalNumberOfForcedFailures)
                {
                    Logger.Log(Level.Warning,
                        "Simulating {0} failure for taskId {1}",
                        FailureType.IsEvaluatorFailure(_failureType) ? "evaluator" : "task",
                        _taskId);
                    if (FailureType.IsEvaluatorFailure(_failureType))
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
            private readonly IIMRUCheckpointHandler _stateHandler;

            [Inject]
            private BroadcastSenderReduceReceiverUpdateFunctionFT(
                [Parameter(typeof(BroadcastReduceConfiguration.NumberOfIterations))] int maxIters,
                [Parameter(typeof(BroadcastReduceConfiguration.Dimensions))] int dim,
                [Parameter(typeof(BroadcastReduceConfiguration.NumWorkers))] int numWorkers,
                [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
                IIMRUCheckpointHandler stateHandler,
                ITaskState taskState)
            {
                _maxIters = maxIters;
                _iterations = 0;
                _dim = dim;
                _intArr = new int[_dim];
                _workers = numWorkers;
                _taskState = (UpdateTaskState<int[], int[]>)taskState;

                _stateHandler = stateHandler;

                int retryNumber;
                int.TryParse(taskId[taskId.Length - 1].ToString(), out retryNumber);
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
                RestoreState();

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
                PersistState();
            }

            /// <summary>
            /// Save the result to Task State
            /// </summary>
            /// <param name="value"></param>
            private void SaveResult(int[] value)
            {
                _taskState.Iterations = _iterations;
                _taskState.Result = value;
                PersistState();
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

            private void PersistState()
            {
                Logger.Log(Level.Info, "SaveState:currentState: {0}", JsonConvert.SerializeObject(_taskState));
                _stateHandler.Persist(_taskState);
            }

            private void RestoreState()
            {
                var obj = (UpdateTaskState<int[], int[]>)_stateHandler.Restore();

                if (obj != null)
                {
                    Logger.Log(Level.Info,
                        "RestoreState:DeserializeObject: input: {0}, iteration: {1}, result: {2}.",
                        obj.Input == null ? string.Empty : string.Join(",", obj.Input),
                        obj.Iterations,
                        obj.Result == null ? string.Empty : string.Join(",", obj.Result));

                    _taskState.Update(obj);
                }
            }
        }
    }
}
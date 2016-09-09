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
using System.IO;
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce
{
    /// <summary>
    /// IMRU program that performs broadcast and reduce with fault tolerance.
    /// </summary>
    public sealed class FaultTolerantPipelinedBroadcastAndReduce : PipelinedBroadcastAndReduce
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(FaultTolerantPipelinedBroadcastAndReduce));

        [Inject]
        private FaultTolerantPipelinedBroadcastAndReduce(IIMRUClient imruClient) : base(imruClient)
        {
        }

        /// <summary>
        /// Runs the actual broadcast and reduce job with fault tolerance
        /// </summary>
        internal void Run(int numberofMappers, int chunkSize, int numIterations, int dim, int mapperMemory, int updateTaskMemory, int maxRetryNumberInRecovery, int totalNumberOfForcedFailures)
        {
            var results = _imruClient.Submit<int[], int[], int[], Stream>(
                BuildJobDefinationBuilder(numberofMappers, chunkSize, numIterations, dim, mapperMemory, updateTaskMemory)
                    .SetMapFunctionConfiguration(BuildMapperFunctionConfig(maxRetryNumberInRecovery, totalNumberOfForcedFailures))
                    .SetMaxRetryNumberInRecovery(maxRetryNumberInRecovery)
                    .Build());
        }

        /// <summary>
        /// Build a test mapper function configuration
        /// </summary>
        /// <param name="maxRetryInRecovery">Number of retries done if first run failed.</param>
        /// <param name="totalNumberOfForcedFailures">Number of allowed failure times in recovery.</param>
        /// <returns></returns>
        private static IConfiguration BuildMapperFunctionConfig(int maxRetryInRecovery, int totalNumberOfForcedFailures)
        {
            var c1 = IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<TestSenderMapFunction>.Class)
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

        [NamedParameter(Documentation = "Set of task ids which will produce task/evaluator failure")]
        internal class TaskIdsToFail : Name<ISet<string>>
        {
        }

        [NamedParameter(Documentation = "Type of failure to simulate")]
        internal class FailureType : Name<int>
        {
            internal static readonly int EvaluatorFailureDuringTaskExecution = 0;
            internal static readonly int TaskFailureDuringTaskExecution = 1;
            internal static readonly int EvaluatorFailureDuringTaskInitialization = 2;
            internal static readonly int TaskFailureDuringTaskInitialization = 3;

            internal static bool IsEvaluatorFailure(int failureType)
            {
                return failureType == EvaluatorFailureDuringTaskExecution ||
                       failureType == EvaluatorFailureDuringTaskInitialization;
            }
        }

        [NamedParameter(Documentation = "Total number of failures in recovery.", DefaultValue = "2")]
        internal class TotalNumberOfForcedFailures : Name<int>
        {
        }

        /// <summary>
        /// The function is to simulate Evaluator/Task failure for mapper evaluator
        /// </summary>
        internal sealed class TestSenderMapFunction : IMapFunction<int[], int[]>
        {
            private int _iterations;
            private readonly string _taskId;
            private readonly ISet<string> _taskIdsToFail;
            private readonly int _failureType;
            private readonly int _maxRetryInRecovery;
            private readonly int _totalNumberOfForcedFailures;
            private readonly int _retryIndex;

            [Inject]
            private TestSenderMapFunction(
                [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
                [Parameter(typeof(TaskIdsToFail))] ISet<string> taskIdsToFail,
                [Parameter(typeof(FailureType))] int failureType,
                [Parameter(typeof(MaxRetryNumberInRecovery))] int maxRetryNumberInRecovery,
                [Parameter(typeof(TotalNumberOfForcedFailures))] int totalNumberOfForcedFailures)
            {
                _taskId = taskId;
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
            /// </summary>
            /// <param name="mapInput">integer array</param>
            /// <returns>The same integer array</returns>
            int[] IMapFunction<int[], int[]>.Map(int[] mapInput)
            {
                _iterations++;
                Logger.Log(Level.Info, "Received value {0} in iteration {1}.", mapInput[0], _iterations);

                if (_failureType == FailureType.EvaluatorFailureDuringTaskExecution ||
                    _failureType == FailureType.TaskFailureDuringTaskExecution)
                {
                    SimulateFailure(10);
                }

                if (mapInput[0] != _iterations)
                {
                    Exceptions.Throw(
                        new Exception("Expected value in mappers (" + _iterations + ") different from actual value (" +
                                      mapInput[0] + ")"),
                        Logger);
                }

                return mapInput;
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
    }
}
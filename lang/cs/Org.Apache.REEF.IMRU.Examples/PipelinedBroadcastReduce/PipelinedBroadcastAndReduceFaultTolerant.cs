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
    /// IMRU program that performs broadcast and reduce with fault tolerant
    /// </summary>
    public class PipelinedBroadcastAndReduceFaultTolerant : PipelinedBroadcastAndReduce
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PipelinedBroadcastAndReduceFaultTolerant));

        [Inject]
        protected PipelinedBroadcastAndReduceFaultTolerant(IIMRUClient imruClient) : base(imruClient)
        {
        }
        
        /// <summary>
        /// Build a test mapper function configuration
        /// </summary>
        /// <param name="maxRetryInRecovery"></param>
        /// <returns></returns>
        protected override IConfiguration BuildMapperFunctionConfig(int maxRetryInRecovery)
        {
            var c1 = IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<TestSenderMapFunction>.Class)
                .Build();

            var c2 = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<TaskIdsToFail, string>(GenericType<TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-2-")
                ////.BindSetEntry<TaskIdsToFail, string>(GenericType<TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-3-")
                .BindIntNamedParam<FailureType>("0")
                .BindNamedParameter(typeof(MaxRetryNumberInRecovery), maxRetryInRecovery.ToString())
                .Build();

            return Configurations.Merge(c1, c2);
        }

        [NamedParameter]
        internal class TaskIdsToFail : Name<ISet<string>>
        {
        }

        [NamedParameter(Documentation = "Type of failure to simulate: 0/1 - evaluator/task during task execution, 2/3 - during task initialization")]
        internal class FailureType : Name<int>
        {
        }

        /// <summary>
        /// The function is to simulate Evaluator/Task failure
        /// </summary>
        internal sealed class TestSenderMapFunction : IMapFunction<int[], int[]>
        {
            private int _iterations;
            private readonly string _taskId;
            private readonly ISet<string> _taskIdsToFail;
            private int _failureType;
            private readonly int _maxRetryInRecovery;

            [Inject]
            private TestSenderMapFunction(
                [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
                [Parameter(typeof(TaskIdsToFail))] ISet<string> taskIdsToFail,
                [Parameter(typeof(FailureType))] int failureType,
                [Parameter(typeof(MaxRetryNumberInRecovery))] int maxRetryNumberInRecovery)
            {
                _taskId = taskId;
                _taskIdsToFail = taskIdsToFail;
                _failureType = failureType;
                _maxRetryInRecovery = maxRetryNumberInRecovery;
                Logger.Log(Level.Info, "TestSenderMapFunction: TaskId: {0}, _maxRetryInRecovery {1},  Failure type: {2}.", _taskId, _maxRetryInRecovery, _failureType);
                foreach (var n in _taskIdsToFail)
                {
                    Logger.Log(Level.Info, "TestSenderMapFunction: taskIdsToFail: {0}", n);
                }

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
                    _taskIdsToFail.FirstOrDefault(e => _taskId.Equals(e + _maxRetryInRecovery)) == null)
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
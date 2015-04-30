/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Examples.GroupCommunication.PipelineBroadcastReduceDriverAndTasks
{
    public class PipelinedMasterTask : ITask
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PipelinedMasterTask));

        private readonly int _numIters;
        private readonly int _numReduceSenders;
        private readonly int _arraySize;

        private readonly IGroupCommClient _groupCommClient;
        private readonly ICommunicationGroupClient _commGroup;
        private readonly IBroadcastSender<int[]> _broadcastSender;
        private readonly IReduceReceiver<int[]> _sumReducer;

        [Inject]
        public PipelinedMasterTask(
            [Parameter(typeof(GroupTestConfig.NumIterations))] int numIters,
            [Parameter(typeof(GroupTestConfig.NumEvaluators))] int numEvaluators,
            [Parameter(typeof(GroupTestConfig.ArraySize))] int arraySize,
            IGroupCommClient groupCommClient)
        {
            Logger.Log(Level.Info, "Hello from master task");
            _numIters = numIters;
            _numReduceSenders = numEvaluators - 1;
            _arraySize = arraySize;
            _groupCommClient = groupCommClient;

            _commGroup = groupCommClient.GetCommunicationGroup(GroupTestConstants.GroupName);
            _broadcastSender = _commGroup.GetBroadcastSender<int[]>(GroupTestConstants.BroadcastOperatorName);
            _sumReducer = _commGroup.GetReduceReceiver<int[]>(GroupTestConstants.ReduceOperatorName);
            Logger.Log(Level.Info, "finished master task constructor");
        }

        public byte[] Call(byte[] memento)
        {
            int[] intArr = new int[_arraySize];

            for (int i = 1; i <= _numIters; i++)
            {
                for (int j = 0; j < _arraySize; j++)
                {
                    intArr[j] = i;
                }

                _broadcastSender.Send(intArr);
                int[] sum = _sumReducer.Reduce();

                Logger.Log(Level.Info, "Received sum: {0} on iteration: {1}", sum, i);

                int expected = TriangleNumber(i) * _numReduceSenders;

                for (int j = 0; j < intArr.Length; j++)
                {
                    if (sum[j] != TriangleNumber(i) * _numReduceSenders)
                    {
                        throw new Exception("Expected " + expected + " but got " + sum);
                    }
                }
            }

            return null;
        }

        public void Dispose()
        {
            _groupCommClient.Dispose();
        }

        private int TriangleNumber(int n)
        {
            return Enumerable.Range(1, n).Sum();
        }
    }
}

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
using System.Diagnostics;
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

            for (int j = 0; j < _arraySize; j++)
            {
                intArr[j] = j;
            }

            Stopwatch broadcastTime = new Stopwatch();
            Stopwatch reduceTime = new Stopwatch();

            for (int i = 1; i <= _numIters; i++)
            {
                intArr[0] = i;

                if (i == 2)
                {
                    broadcastTime.Reset();
                    reduceTime.Reset();
                }

                broadcastTime.Start();
                _broadcastSender.Send(intArr);
                broadcastTime.Stop();

                reduceTime.Start();
                int[] sum = _sumReducer.Reduce();
                reduceTime.Stop();

                Logger.Log(Level.Info, "Received sum: {0} on iteration: {1} with array length: {2}", sum[0], i,
                    sum.Length);

                int expected = TriangleNumber(i) * _numReduceSenders;

                if (sum[0] != TriangleNumber(i) * _numReduceSenders)
                {
                    throw new Exception("Expected " + expected + " but got " + sum[0]);
                }

                if (i >= 2)
                {
                    var msg = string.Format("Average time (milliseconds) taken for broadcast: {0} and reduce: {1}",
                            broadcastTime.ElapsedMilliseconds / ((double)(i - 1)),
                            reduceTime.ElapsedMilliseconds / ((double)(i - 1)));
                    Logger.Log(Level.Info, msg);
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

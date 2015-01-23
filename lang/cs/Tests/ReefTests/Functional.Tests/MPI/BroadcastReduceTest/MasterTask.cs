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

using Org.Apache.Reef.IO.Network.Group.Operators;
using Org.Apache.Reef.IO.Network.Group.Task;
using Org.Apache.Reef.Tasks;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;
using System;
using System.Linq;

namespace Org.Apache.Reef.Test.Functional.Tests.MPI.BroadcastReduceTest
{
    public class MasterTask : ITask
    {
        private static Logger _logger = Logger.GetLogger(typeof(MasterTask));

        private int _numIters;
        private int _numReduceSenders;

        private IMpiClient _mpiClient;
        private ICommunicationGroupClient _commGroup;
        private IBroadcastSender<int> _broadcastSender;
        private IReduceReceiver<int> _sumReducer;

        [Inject]
        public MasterTask(
            [Parameter(typeof(MpiTestConfig.NumIterations))] int numIters,
            [Parameter(typeof(MpiTestConfig.NumEvaluators))] int numEvaluators,
            IMpiClient mpiClient)
        {
            _logger.Log(Level.Info, "Hello from master task");
            _numIters = numIters;
            _numReduceSenders = numEvaluators - 1;
            _mpiClient = mpiClient;

            _commGroup = mpiClient.GetCommunicationGroup(MpiTestConstants.GroupName);
            _broadcastSender = _commGroup.GetBroadcastSender<int>(MpiTestConstants.BroadcastOperatorName);
            _sumReducer = _commGroup.GetReduceReceiver<int>(MpiTestConstants.ReduceOperatorName);
        }

        public byte[] Call(byte[] memento)
        {
            for (int i = 1; i <= _numIters; i++)
            {
                // Each slave task calculates the nth triangle number
                _broadcastSender.Send(i);
                
                // Sum up all of the calculated triangle numbers
                int sum = _sumReducer.Reduce();
                _logger.Log(Level.Info, "Received sum: {0} on iteration: {1}", sum, i);

                int expected = TriangleNumber(i) * _numReduceSenders;
                if (sum != TriangleNumber(i) * _numReduceSenders)
                {
                    throw new Exception("Expected " + expected + " but got " + sum);
                }
            }

            return null;
        }

        public void Dispose()
        {
            _mpiClient.Dispose();
        }

        private int TriangleNumber(int n)
        {
            return Enumerable.Range(1, n).Sum();
        }
    }
}

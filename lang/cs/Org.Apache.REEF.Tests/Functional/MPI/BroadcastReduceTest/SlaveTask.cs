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

using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.MPI.BroadcastReduceTest
{
    public class SlaveTask : ITask
    {
        private static readonly Logger _logger = Logger.GetLogger(typeof(SlaveTask));

        private readonly int _numIterations;
        private readonly IMpiClient _mpiClient;
        private readonly ICommunicationGroupClient _commGroup;
        private readonly IBroadcastReceiver<int> _broadcastReceiver;
        private readonly IReduceSender<int> _triangleNumberSender;

        [Inject]
        public SlaveTask(
            [Parameter(typeof(MpiTestConfig.NumIterations))] int numIters,
            IMpiClient mpiClient)
        {
            _logger.Log(Level.Info, "Hello from slave task");

            _numIterations = numIters;
            _mpiClient = mpiClient;
            _commGroup = _mpiClient.GetCommunicationGroup(MpiTestConstants.GroupName);
            _broadcastReceiver = _commGroup.GetBroadcastReceiver<int>(MpiTestConstants.BroadcastOperatorName);
            _triangleNumberSender = _commGroup.GetReduceSender<int>(MpiTestConstants.ReduceOperatorName);
        }

        public byte[] Call(byte[] memento)
        {
            for (int i = 0; i < _numIterations; i++)
            {
                // Receive n from Master Task
                int n = _broadcastReceiver.Receive();
                _logger.Log(Level.Info, "Calculating TriangleNumber({0}) on slave task...", n);

                // Calculate the nth Triangle number and send it back to driver
                int triangleNum = TriangleNumber(n);
                _logger.Log(Level.Info, "Sending sum: {0} on iteration {1}.", triangleNum, i);
                _triangleNumberSender.Send(triangleNum);
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

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
using System.Diagnostics;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Examples.GroupCommunication.PipelineBroadcastReduceDriverAndTasks
{
    public class PipelinedSlaveTask : ITask
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PipelinedSlaveTask));

        private readonly int _numIterations;
        private readonly IGroupCommClient _groupCommClient;
        private readonly ICommunicationGroupClient _commGroup;
        private readonly IBroadcastReceiver<int[]> _broadcastReceiver;
        private readonly IReduceSender<int[]> _triangleNumberSender;

        [Inject]
        public PipelinedSlaveTask(
            [Parameter(typeof(GroupTestConfig.NumIterations))] int numIters,
            IGroupCommClient groupCommClient)
        {
            Logger.Log(Level.Info, "Hello from slave task");

            _numIterations = numIters;
            _groupCommClient = groupCommClient;
            _commGroup = _groupCommClient.GetCommunicationGroup(GroupTestConstants.GroupName);
            _broadcastReceiver = _commGroup.GetBroadcastReceiver<int[]>(GroupTestConstants.BroadcastOperatorName);
            _triangleNumberSender = _commGroup.GetReduceSender<int[]>(GroupTestConstants.ReduceOperatorName);
        }

        public byte[] Call(byte[] memento)
        {
            Stopwatch watch1 = new Stopwatch();
            Stopwatch watch2 = new Stopwatch();

            for (int i = 0; i < _numIterations; i++)
            {
                // Receive n from Master Task
                if (i > 0)
                {
                    watch1.Start();
                }
                int[] intVec = _broadcastReceiver.Receive();

                if (i > 0)
                {
                    watch1.Stop();
                }

                Logger.Log(Level.Info, "Calculating TriangleNumber({0}) on slave task...", intVec[0]);

                // Calculate the nth Triangle number and send it back to driver
                int triangleNum = TriangleNumber(intVec[0]);
                Logger.Log(Level.Info, "Sending sum: {0} on iteration {1} with array length {2}.", triangleNum, i, intVec.Length);

                int[] resArr = new int[intVec.Length];

                for (int j = 0; j < resArr.Length; j++)
                {
                    resArr[j] = triangleNum;
                }

                if (i > 0)
                {
                    watch2.Start();
                }
                _triangleNumberSender.Send(resArr);
                if (i > 0)
                {
                    watch2.Stop();
                }
            }

            Logger.Log(Level.Info, "$$$$$$$$$$$ timings are {0} {1}", watch1.ElapsedMilliseconds / 1000.0, watch2.ElapsedMilliseconds / 1000.0);

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

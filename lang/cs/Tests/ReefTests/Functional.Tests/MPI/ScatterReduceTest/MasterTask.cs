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
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.Reef.Test.Functional.Tests.MPI.ScatterReduceTest
{
    public class MasterTask : ITask
    {
        private static Logger _logger = Logger.GetLogger(typeof(MasterTask));

        private IMpiClient _mpiClient;
        private ICommunicationGroupClient _commGroup;
        private IScatterSender<int> _scatterSender;
        private IReduceReceiver<int> _sumReducer;

        [Inject]
        public MasterTask(IMpiClient mpiClient)
        {
            _logger.Log(Level.Info, "Hello from master task");
            _mpiClient = mpiClient;

            _commGroup = mpiClient.GetCommunicationGroup(MpiTestConstants.GroupName);
            _scatterSender = _commGroup.GetScatterSender<int>(MpiTestConstants.ScatterOperatorName);
            _sumReducer = _commGroup.GetReduceReceiver<int>(MpiTestConstants.ReduceOperatorName);
        }

        public byte[] Call(byte[] memento)
        {
            List<int> data = Enumerable.Range(1, 100).ToList();
            List<string> order = GetScatterOrder();
            _scatterSender.Send(data, order);

            int sum = _sumReducer.Reduce();
            _logger.Log(Level.Info, "Received sum: {0}", sum);

            return null;
        }

        public void Dispose()
        {
            _mpiClient.Dispose();
        }

        private List<string> GetScatterOrder()
        {
            return new List<string> { "SlaveTask-4", "SlaveTask-3", "SlaveTask-2", "SlaveTask-1" };
        }
    }
}

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

using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Examples.GroupCommunication.ScatterReduceDriverAndTasks
{
    public class SlaveTask : ITask
    {
        private static readonly Logger _logger = Logger.GetLogger(typeof(SlaveTask));

        private readonly IGroupCommClient _groupCommClient;
        private readonly ICommunicationGroupClient _commGroup;
        private readonly IScatterReceiver<int> _scatterReceiver;
        private readonly IReduceSender<int> _sumSender;

        [Inject]
        public SlaveTask(IGroupCommClient groupCommClient)
        {
            _logger.Log(Level.Info, "Hello from slave task");

            _groupCommClient = groupCommClient;
            _commGroup = _groupCommClient.GetCommunicationGroup(GroupTestConstants.DefaultGroupName);
            _scatterReceiver = _commGroup.GetScatterReceiver<int>(GroupTestConstants.ScatterOperatorName);
            _sumSender = _commGroup.GetReduceSender<int>(GroupTestConstants.ReduceOperatorName);
        }

        public byte[] Call(byte[] memento)
        {
            List<int> data = _scatterReceiver.Receive();
            _logger.Log(Level.Info, "Received data: {0}", string.Join(" ", data));

            int sum = data.Sum();
            _logger.Log(Level.Info, "Sending back sum: {0}", sum);
            _sumSender.Send(sum);

            return null;
        }

        public void Dispose()
        {
            _groupCommClient.Dispose();
        }
    }
}

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
    public class MasterTask : ITask
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MasterTask));

        private readonly IGroupCommClient _groupCommClient;
        private readonly ICommunicationGroupClient _commGroup;
        private readonly IScatterSender<int> _scatterSender;
        private readonly IReduceReceiver<int> _sumReducer;

        [Inject]
        public MasterTask(IGroupCommClient groupCommClient)
        {
            Logger.Log(Level.Info, "Hello from master task");
            _groupCommClient = groupCommClient;

            _commGroup = groupCommClient.GetCommunicationGroup(GroupTestConstants.GroupName);
            _scatterSender = _commGroup.GetScatterSender<int>(GroupTestConstants.ScatterOperatorName);
            _sumReducer = _commGroup.GetReduceReceiver<int>(GroupTestConstants.ReduceOperatorName);
        }

        public byte[] Call(byte[] memento)
        {
            _groupCommClient.Initialize();
            List<int> data = Enumerable.Range(1, 100).ToList();
            _scatterSender.Send(data);

            int sum = _sumReducer.Reduce();
            Logger.Log(Level.Info, "Received sum: {0}", sum);

            return null;
        }

        public void Dispose()
        {
            _groupCommClient.Dispose();
        }

        private List<string> GetScatterOrder()
        {
            return new List<string> { "SlaveTask-4", "SlaveTask-3", "SlaveTask-2", "SlaveTask-1" };
        }
    }
}

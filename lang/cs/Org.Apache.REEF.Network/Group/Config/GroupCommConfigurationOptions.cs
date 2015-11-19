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

using System.Collections.Generic;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Group.Config
{
    public class GroupCommConfigurationOptions
    {
        [NamedParameter("Name of the communication group")]
        public class CommunicationGroupName : Name<string>
        {
        }

        [NamedParameter("Name of the Group Communication operator")]
        public class OperatorName : Name<string>
        {
        }

        [NamedParameter("Driver identifier")]
        public class DriverId : Name<string>
        {
        }

        [NamedParameter("Timeout for receiving data", defaultValue: "50000")]
        public class Timeout : Name<int>
        {
        }

        /// <summary>
        /// Each Communication group needs to check and wait until all the other nodes in the group are registered to the NameServer
        /// Sleep time is set between each retry. 
        /// </summary>
        [NamedParameter("sleep time to wait for nodes to be registered", defaultValue: "500")]
        internal sealed class SleepTimeWaitingForRegistration : Name<int>
        {
        }

        /// <summary>
        /// Each Communication group needs to check and wait until all the other nodes in the group are registered to the NameServer
        /// </summary>
        /// <remarks>
        /// When there are many nodes, e.g over 100, the waiting time might be pretty long. 
        /// We don't want to set it too low in case some nodes are just slow, if we simply throw an exception, that is not right. 
        /// We don't want it to try endlessly in case some node is really dead, we should come out with exception. 
        /// We want it to return as soon as all nodes in the group are registered, So increasing retry count is better than increasing sleep time.
        /// Current default sleep time is 500ms. Default retry is 500. Total is 250000ms, that is 250s, little bit more than 4 min
        /// </remarks>
        [NamedParameter("Retry times to wait for nodes to be registered", defaultValue: "500")]
        internal sealed class RetryCountWaitingForRegistration : Name<int>
        {
        }

        [NamedParameter("Master task identifier")]
        public class MasterTaskId : Name<string>
        {
        }

        [NamedParameter("Group name", defaultValue: "Group1")]
        public class GroupName : Name<string>
        {
        }

        [NamedParameter("Number of tasks", defaultValue: "5")]
        public class NumberOfTasks : Name<int>
        {
        }

        [NamedParameter("with of the tree in topology", defaultValue: "2")]
        public class FanOut : Name<int>
        {
        }

        [NamedParameter("Serialized communication group configuration")]
        public class SerializedGroupConfigs : Name<ISet<string>>
        {
        }

        [NamedParameter("Serialized operator configuration")]
        public class SerializedOperatorConfigs : Name<ISet<string>>
        {
        }

        [NamedParameter("Id of root task in operator topology")]
        public class TopologyRootTaskId : Name<string>
        {
        }

        [NamedParameter("Ids of child tasks in operator topology")]
        public class TopologyChildTaskIds : Name<ISet<string>>
        {
        }

        [NamedParameter("Type of the message")]
        public class MessageType : Name<string>
        {
        }

        [NamedParameter("Wether or not to call topology initialize", defaultValue: "true")]
        public class Initialize : Name<bool>
        {
        }
    }
}

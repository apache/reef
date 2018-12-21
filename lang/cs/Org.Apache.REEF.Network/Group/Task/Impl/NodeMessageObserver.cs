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
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.NetworkService;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    /// An observer for a node in a group communication graph.
    /// </summary>
    internal sealed class NodeMessageObserver<T> : IObserver<NsMessage<GeneralGroupCommunicationMessage>>
    {
        private readonly NodeStruct<T> _nodeStruct;

        internal NodeMessageObserver(NodeStruct<T> nodeStruct)
        {
            _nodeStruct = nodeStruct;
        }

        /// <summary>
        /// Add data into the queue.
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(NsMessage<GeneralGroupCommunicationMessage> value)
        {
            var gcMessage = value.Data as GroupCommunicationMessage<T>;
            if (gcMessage != null && gcMessage.Data != null && gcMessage.Data.Length > 0)
            {
                _nodeStruct.AddData(gcMessage);
            }
        }

        /// <summary>
        /// Gets the group name of the node.
        /// </summary>
        public string GroupName
        {
            get { return _nodeStruct.GroupName; }
        }

        /// <summary>
        /// Gets the operator name of the node.
        /// </summary>
        public string OperatorName
        {
            get { return _nodeStruct.OperatorName; }
        }

        public void OnError(Exception error)
        {
            // TODO[JIRA REEF-1407]: Cancel on queue of node and handle error in application layer.
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            // TODO[JIRA REEF-1407]: Complete adding on queue of node.
            throw new NotImplementedException();
        }
    }
}
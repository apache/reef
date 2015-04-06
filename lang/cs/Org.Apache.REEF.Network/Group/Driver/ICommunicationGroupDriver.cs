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
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.Group.Driver
{
    /// <summary>
    /// Used to configure MPI operators in Reef driver.
    /// All operators in the same Communication Group run on the the 
    /// same set of tasks.
    /// </summary>
    public interface ICommunicationGroupDriver
    {
        /// <summary>
        /// Returns the list of task ids that belong to this Communication Group
        /// </summary>
        List<string> TaskIds { get; }

        /// <summary>
        /// Adds the Broadcast MPI operator to the communication group.
        /// </summary>
        /// <typeparam name="TMessage">The type of messages that operators will send</typeparam>
        /// <typeparam name="TMessageCodec">The codec used for serializing messages</typeparam>
        /// <param name="operatorName">The name of the broadcast operator</param>
        /// <param name="masterTaskId">The master task id in broadcast operator</param>
        /// <param name="topologyType">The topology type for the operator</param>
        /// <returns>The same CommunicationGroupDriver with the added Broadcast operator info</returns>
        ICommunicationGroupDriver AddBroadcast<TMessage, TMessageCodec>(string operatorName, string masterTaskId, TopologyTypes topologyType = TopologyTypes.Flat) where TMessageCodec : ICodec<TMessage>;

        /// <summary>
        /// Adds the Broadcast MPI operator to the communication group. Default to IntCodec
        /// </summary>
        /// <param name="operatorName">The name of the broadcast operator</param>
        /// <param name="masterTaskId">The master task id in broadcast operator</param>
        /// <param name="topologyType">The topology type for the operator</param>
        /// <returns>The same CommunicationGroupDriver with the added Broadcast operator info</returns>
        ICommunicationGroupDriver AddBroadcast(string operatorName, string masterTaskId, TopologyTypes topologyType = TopologyTypes.Flat);

        /// <summary>
        /// Adds the Reduce MPI operator to the communication group.
        /// </summary>
        /// <typeparam name="TMessage">The type of messages that operators will send</typeparam>
        /// <typeparam name="TMessageCodec">The codec used for serializing messages</typeparam>
        /// <param name="operatorName">The name of the reduce operator</param>
        /// <param name="masterTaskId">The master task id for the typology</param>
        /// <param name="reduceFunction">The class used to aggregate all messages.</param>
        /// <param name="topologyType">The topology for the operator</param>
        /// <returns>The same CommunicationGroupDriver with the added Reduce operator info</returns>
        ICommunicationGroupDriver AddReduce<TMessage, TMessageCodec>(string operatorName, string masterTaskId, IReduceFunction<TMessage> reduceFunction, TopologyTypes topologyType = TopologyTypes.Flat) where TMessageCodec : ICodec<TMessage>;

        /// <summary>
        /// Adds the Reduce MPI operator to the communication group with default IntCodec
        /// </summary>
        /// <param name="operatorName">The name of the reduce operator</param>
        /// <param name="masterTaskId">The master task id for the typology</param>
        /// <param name="reduceFunction">The class used to aggregate all messages.</param>
        /// <param name="topologyType">The topology for the operator</param>
        /// <returns>The same CommunicationGroupDriver with the added Reduce operator info</returns>
        ICommunicationGroupDriver AddReduce(string operatorName, string masterTaskId, IReduceFunction<int> reduceFunction, TopologyTypes topologyType = TopologyTypes.Flat);

        /// <summary>
        /// Adds the Scatter MPI operator to the communication group.
        /// </summary>
        /// <typeparam name="TMessage">The type of messages that operators will send</typeparam>
        /// <typeparam name="TMessageCodec">The codec used for serializing messages</typeparam>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="senderId">The sender id</param>
        /// <param name="topologyType">type of topology used in the operaor</param>
        /// <returns>The same CommunicationGroupDriver with the added Scatter operator info</returns>
        ICommunicationGroupDriver AddScatter<TMessage, TMessageCodec>(string operatorName, string senderId, TopologyTypes topologyType = TopologyTypes.Flat) where TMessageCodec : ICodec<TMessage>;

        /// <summary>
        /// Adds the Scatter MPI operator to the communication group with default Codec
        /// </summary>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="senderId">The sender id</param>
        /// <param name="topologyType">type of topology used in the operaor</param>
        /// <returns>The same CommunicationGroupDriver with the added Scatter operator info</returns>
        ICommunicationGroupDriver AddScatter(string operatorName, string senderId, TopologyTypes topologyType = TopologyTypes.Flat);

        /// <summary>
        /// Finalizes the CommunicationGroupDriver.
        /// After the CommunicationGroupDriver has been finalized, no more operators may
        /// be added to the group.
        /// </summary>
        /// <returns>The same finalized CommunicationGroupDriver</returns>
        ICommunicationGroupDriver Build();

        /// <summary>
        /// Add a task to the communication group.
        /// The CommunicationGroupDriver must have called Build() before adding tasks to the group.
        /// </summary>
        /// <param name="taskId">The id of the task to add</param>
        void AddTask(string taskId);

        /// <summary>
        /// Get the Task Configuration for this communication group. 
        /// Must be called only after all tasks have been added to the CommunicationGroupDriver.
        /// </summary>
        /// <param name="taskId">The task id of the task that belongs to this Communication Group</param>
        /// <returns>The Task Configuration for this communication group</returns>
        IConfiguration GetGroupTaskConfiguration(string taskId);
    }
}

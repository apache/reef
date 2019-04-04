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
using Org.Apache.REEF.Tang.Interface;
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using System.Linq;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Enum;
using Org.Apache.REEF.Tang.Implementations.Tang;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl
{
    /// <summary>
    /// Topology class for N nodes organized as a shallow tree with 1 root (the master) and N-1 nodes
    /// connected to it.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public class FlatTopology : ITopology
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(FlatTopology));

        private string _rootTaskId = null;
        private readonly int _rootId;
        private string _taskStage = null;
        private volatile int _iteration = 1;
        private bool _finalized = false;

        private readonly IDictionary<int, DataNode> _nodes;
        private DataNode _root; // This is just for caching
        private readonly HashSet<string> _lostNodesToBeRemoved = new HashSet<string>();
        private HashSet<string> _nodesWaitingToJoinTopologyNextIteration = new HashSet<string>();
        private HashSet<string> _nodesWaitingToJoinTopology = new HashSet<string>();

        private volatile int _availableDataPoints = 0;
        private int _totalDataPoints = 0;

        private readonly object _lock = new object();

        /// <summary>
        /// Constructor for flat topology. After construction the graph is empty
        /// and tasks need to be added.
        /// </summary>
        /// <param name="rootId">The id of the task that will be set as root of the topology</param>
        /// <param name="sorted">Whether the leaf nodes need to be ordered or not</param>
        public FlatTopology(int rootId, bool sorted = false)
        {
            _rootId = rootId;
            OperatorId = -1;

            if (sorted)
            {
                _nodes = new SortedDictionary<int, DataNode>();
            }
            else
            {
                _nodes = new Dictionary<int, DataNode>();
            }
        }

        /// <summary>
        /// The identifier of the operator using the topology.
        /// </summary>
        public int OperatorId { get; set; }

        /// <summary>
        /// The stage of the operator using the topology.
        /// </summary>
        public string StageName { get; set; }

        /// <summary>
        /// Adds a new task to the topology.
        /// When called before Build() actually adds the task to the topology.
        /// After Build(), it assumes that the task is added because recovered from a failure.
        /// A failure machine is given as input so that the topology can update the number of available nodes.
        /// </summary>
        /// <param name="taskId">The id of the task to be added</param>
        /// <param name="failureMachine">The failure machine that manage the failure for the operator.</param>
        /// <returns>True if is the first time the topology sees this task</returns>
        public bool AddTask(string taskId, IFailureStateMachine failureMachine)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException(taskId);
            }

            if (failureMachine == null)
            {
                throw new ArgumentNullException(nameof(failureMachine));
            }

            var id = Utils.GetTaskNum(taskId);

            lock (_lock)
            {
                if (_nodes.TryGetValue(id, out DataNode node))
                {
                    if (node.FailState != DataNodeState.Reachable)
                    {
                        // This is node already added to the topology and which probably failed.
                        _nodesWaitingToJoinTopologyNextIteration.Add(taskId);
                        node.FailState = DataNodeState.Unreachable;
                        return false;
                    }

                    throw new ArgumentException("Task already added to the topology.");
                }

                DataNode dnode = new DataNode(id, false);
                _nodes[id] = dnode;

                if (_finalized)
                {
                    // New node but elastically added. It should be gracefully added to the topology.
                    _nodesWaitingToJoinTopologyNextIteration.Add(taskId);
                    dnode.FailState = DataNodeState.Unreachable;
                    _root.Children.Add(dnode);
                    failureMachine.AddDataPoints(1, true);
                    failureMachine.RemoveDataPoints(1);
                    return false;
                }

                // This is required later in order to build the topology
                if (_taskStage == string.Empty)
                {
                    _taskStage = Utils.GetTaskStages(taskId);
                }
            }

            _availableDataPoints++;
            failureMachine.AddDataPoints(1, true);

            return true;
        }

        /// <summary>
        /// Removes a task from the topology.
        /// </summary>
        /// <param name="taskId">The id of the task to be removed</param>
        /// <returns>The number of data points lost because of the removed task</returns>
        public int RemoveTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException(nameof(taskId));
            }

            if (taskId == _rootTaskId)
            {
                throw new NotImplementedException("Failure on master not supported yet");
            }

            var id = Utils.GetTaskNum(taskId);

            lock (_lock)
            {
                if (!_nodes.TryGetValue(id, out DataNode node))
                {
                    throw new ArgumentException("Task is not part of this topology");
                }

                var prevState = node.FailState;
                node.FailState = DataNodeState.Lost;
                _nodesWaitingToJoinTopologyNextIteration.Remove(taskId);
                _nodesWaitingToJoinTopology.Remove(taskId);
                _lostNodesToBeRemoved.Add(taskId);

                if (prevState != DataNodeState.Reachable)
                {
                    return 0;
                }

                _availableDataPoints--;
            }

            return 1;
        }

        /// <summary>
        /// Whether the topology can be sheduled.
        /// </summary>
        /// <returns>True if the topology is ready to be scheduled</returns>
        public bool CanBeScheduled()
        {
            return _root != null;
        }

        /// <summary>
        /// Finalizes the topology.
        /// After the topology has been finalized, any task added to the topology is
        /// assumed as a task recovered from a failure.
        /// </summary>
        /// <returns>The same finalized topology</returns>
        public ITopology Build()
        {
            if (_finalized)
            {
                throw new IllegalStateException("Topology cannot be built more than once");
            }

            if (_root == null)
            {
                throw new IllegalStateException("Topology cannot be built because the root node is missing");
            }

            if (OperatorId <= 0)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any operator");
            }

            if (StageName == string.Empty)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any stage");
            }

            BuildTopology();

            _rootTaskId = Utils.BuildTaskId(_taskStage, _rootId);
            _finalized = true;

            return this;
        }

        /// <summary>
        /// Utility method for logging the topology state.
        /// This will be called every time a topology object is built or modified
        /// because of a failure.
        /// </summary>
        public string LogTopologyState()
        {
            return _rootId + "\n" + string.Join(" ", _root.Children.Select(node =>
                node.FailState == DataNodeState.Reachable ? "" + node.TaskId : "X"));
        }

        /// <summary>
        /// Adds the topology configuration for the input task to the input builder.
        /// Must be called only after all tasks have been added to the topology, i.e., after build.
        /// </summary>
        /// <param name="taskId">The task id of the task that belongs to this Topology</param>
        /// <returns>The task configuration</returns>
        public IConfiguration GetTaskConfiguration(int taskId)
        {
            if (!_finalized)
            {
                throw new IllegalStateException("Cannot get task configuration from a not finalized topology.");
            }

            var confBuilder = TangFactory.GetTang().NewConfigurationBuilder();

            if (taskId == _rootId)
            {
                foreach (var tId in _root.Children)
                {
                    confBuilder.BindSetEntry<Config.OperatorParameters.TopologyChildTaskIds, int>("" + tId.TaskId);
                }
            }

            return confBuilder
                .BindNamedParam<Config.OperatorParameters.TopologyRootTaskId, int>("" + _rootId)
                .Build();
        }

        /// <summary>
        /// This method is triggered when a node contacts the driver to synchronize the remote topology
        /// with the driver's one.
        /// </summary>
        /// <param name="taskId">The identifier of the task asking for the update</param>
        /// <param name="failureStateMachine">An optional failure machine to log updates</param>
        public IEnumerable<IElasticDriverMessage> TopologyUpdateResponse(
            string taskId,
            Optional<IFailureStateMachine> failureStateMachine)
        {
            if (taskId != _rootTaskId)
            {
                throw new IllegalStateException("Only root tasks are supposed to request topology updates.");
            }

            if (!failureStateMachine.IsPresent())
            {
                throw new IllegalStateException("Cannot update topology without failure machine.");
            }

            lock (_lock)
            {
                var update = new TopologyUpdate(_rootTaskId, _nodesWaitingToJoinTopology);
                var data = new UpdateMessagePayload( new[]{ update }, StageName, OperatorId, _iteration);
                var returnMessage = new ElasticDriverMessageImpl(_rootTaskId, data);

                if (_nodesWaitingToJoinTopology.Count > 0)
                {
                    if (Log.IsLoggable(Level.Info))
                    {
                        Log.Log(Level.Info,
                            "Tasks [{0}] are added to topology in iteration {1}",
                            string.Join(",", _nodesWaitingToJoinTopology),
                            _iteration);
                    }

                    _availableDataPoints += _nodesWaitingToJoinTopology.Count;
                    failureStateMachine.Value.AddDataPoints(_nodesWaitingToJoinTopology.Count, false);

                    foreach (var node in _nodesWaitingToJoinTopology)
                    {
                        var id = Utils.GetTaskNum(node);
                        _nodes[id].FailState = DataNodeState.Reachable;
                    }

                    _nodesWaitingToJoinTopology.Clear();
                }

                return new[] { returnMessage };
            }
        }

        /// <summary>
        /// Action to trigger when the operator receives a notification that a new iteration is started.
        /// </summary>
        /// <param name="iteration">The new iteration number</param>
        public void OnNewIteration(int iteration)
        {
            Log.Log(Level.Info,
                "Flat Topology for Operator {0} in Iteration {1} is closed with {2} nodes",
                OperatorId,
                iteration - 1,
                _availableDataPoints);
            _iteration = iteration;
            _totalDataPoints += _availableDataPoints;

            lock (_lock)
            {
                _nodesWaitingToJoinTopology = _nodesWaitingToJoinTopologyNextIteration;
                _nodesWaitingToJoinTopologyNextIteration = new HashSet<string>();
            }
        }

        /// <summary>
        /// Reconfigure the topology in response to some event.
        /// </summary>
        /// <param name="taskId">The task id responsible for the topology change</param>
        /// <param name="info">Some additional topology-specific information</param>
        /// <param name="iteration">The optional iteration number in which the event occurred</param>
        /// <returns>One or more messages for reconfiguring the Tasks</returns>
        public IEnumerable<IElasticDriverMessage> Reconfigure(
            string taskId,
            string info = null, 
            int? iteration = null)
        {
            if (taskId == _rootTaskId)
            {
                throw new NotImplementedException("Failure on master not supported yet.");
            }

            List<IElasticDriverMessage> messages = new List<IElasticDriverMessage>();

            lock (_lock)
            {
                var update = new TopologyUpdate(_rootTaskId, _lostNodesToBeRemoved);
                var data = new FailureMessagePayload(new[] { update }, StageName, OperatorId, -1);
                var returnMessage = new ElasticDriverMessageImpl(_rootTaskId, data);

                Log.Log(Level.Info, "Task {0} is removed from topology", taskId);
                messages.Add(returnMessage);
                _lostNodesToBeRemoved.Clear();
            }

            return messages;
        }

        /// <summary>
        /// Log the final statistics of the operator.
        /// This is called when the pipeline execution is completed.
        /// </summary>
        public string LogFinalStatistics()
        {
            return string.Format(
                "\nAverage number of nodes in the topology of Operator {0}: {1}",
                OperatorId,
                _iteration >= 2 ? (float)_totalDataPoints / (_iteration - 1) : _availableDataPoints);
        }

        private void BuildTopology()
        {
            _root = _nodes[_rootId];
            _root.AddChildren(_nodes.Values.Where(n => n.TaskId != _rootId));
        }
    }
}
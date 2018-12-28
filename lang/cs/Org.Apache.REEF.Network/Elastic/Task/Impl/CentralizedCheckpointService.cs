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

using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Checkpointing service used to locally checkpoint some task state or retrieve previously checkpointed local / remote states.
    /// This service allows to reach remote checkpoints stored in root node when operators support it.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal class CentralizedCheckpointLayer : ICheckpointLayer
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(CentralizedCheckpointLayer));

        private readonly ConcurrentDictionary<CheckpointIdentifier, SortedDictionary<int, ICheckpointState>> _checkpoints;
        private readonly ConcurrentDictionary<CheckpointIdentifier, string> _roots;
        private readonly ConcurrentDictionary<CheckpointIdentifier, ManualResetEvent> _checkpointsWaiting;

        private readonly int _limit;
        private readonly int _timeout;
        private readonly int _retry;

        private readonly CancellationSource _cancellationSource;

        [Inject]
        private CentralizedCheckpointLayer(
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumCheckpoints))] int num,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Retry))] int retry,
            CancellationSource cancellationSource,
            CommunicationLayer commLayer)
        {
            _limit = num;
            _timeout = timeout;
            _retry = retry;

            _cancellationSource = cancellationSource;

            _checkpoints = new ConcurrentDictionary<CheckpointIdentifier, SortedDictionary<int, ICheckpointState>>();
            _roots = new ConcurrentDictionary<CheckpointIdentifier, string>();
            _checkpointsWaiting = new ConcurrentDictionary<CheckpointIdentifier, ManualResetEvent>();
        }

        /// The service for communicating with the other available nodes.
        /// </summary>
        public CommunicationLayer CommunicationLayer { private get; set; }

        /// <summary>
        /// Register the current task id as well as notify the root task for the operator.
        /// </summary>
        /// <param name="stageName">The name of the stage</param>
        /// <param name="operatorId">The operator identifier</param>
        /// <param name="taskId">The identifier of the current task</param>
        /// <param name="rootTaskId">The identifier of the root task of the operator</param>
        public void RegisterNode(string stageName, int operatorId, string taskId, string rootTaskId)
        {
            var id = new CheckpointIdentifier(stageName, operatorId);
            if (!_roots.ContainsKey(id) && taskId != rootTaskId)
            {
                _roots.TryAdd(id, rootTaskId);
            }
        }

        /// <summary>
        /// Checkpoint the input state.
        /// </summary>
        /// <param name="state">The state to checkpoint</param>
        public void Checkpoint(ICheckpointState state)
        {
            if (state.StageName == null || state.StageName == string.Empty)
            {
                throw new ArgumentException(nameof(state.StageName), "Null or empty.");
            }

            if (state.OperatorId < 0)
            {
                throw new ArgumentException(nameof(state.OperatorId), "Invalid.");
            }

            SortedDictionary<int, ICheckpointState> checkpoints;
            var id = new CheckpointIdentifier(state.StageName, state.OperatorId);
            ManualResetEvent waiting;

            if (!_checkpoints.TryGetValue(id, out checkpoints))
            {
                checkpoints = new SortedDictionary<int, ICheckpointState>();
                _checkpoints.TryAdd(id, checkpoints);
            }

            checkpoints[state.Iteration] = state;

            if (_checkpointsWaiting.TryRemove(id, out waiting))
            {
                waiting.Set();
            }

            CheckSize(checkpoints);
        }

        /// <summary>
        /// Retrieve a checkpoint.
        /// </summary>
        /// <param name="checkpoint">The retrieve checkpoint if exists</param>
        /// <param name="taskId">The local task identifier</param>
        /// <param name="stageName">The name of the stage</param>
        /// <param name="operatorId">The operator identifier</param>
        /// <param name="iteration">The iteration number of the checkpoint</param>
        /// <param name="requestToRemote">Whether to request the checkpoint remotely if not found locally</param>
        /// <returns>True if the checkpoint is found, false otherwise</returns>
        public bool GetCheckpoint(out ICheckpointState checkpoint, string taskId, string stageName, int operatorId, int iteration = -1, bool requestToRemote = true)
        {
            SortedDictionary<int, ICheckpointState> checkpoints;
            var id = new CheckpointIdentifier(stageName, operatorId);
            checkpoint = null;

            if (!_checkpoints.TryGetValue(id, out checkpoints))
            {
                LOGGER.Log(Level.Warning, "Asking for a checkpoint not in the service.");

                if (!requestToRemote)
                {
                    LOGGER.Log(Level.Warning, "Trying to recover from a non existing checkpoint.");
                    return false;
                }

                string rootTaskId;

                if (!_roots.TryGetValue(id, out rootTaskId))
                {
                    LOGGER.Log(Level.Warning, "Trying to recover from a non existing checkpoint.");
                    return false;
                }

                if (CommunicationLayer == null)
                {
                    throw new IllegalStateException("Communication service not set up.");
                }

                var received = new ManualResetEvent(false);
                var retry = 0;

                do
                {
                    LOGGER.Log(Level.Info, $"Retrieving the checkpoint from {rootTaskId}.");
                    var cpm = new CheckpointMessageRequest(stageName, operatorId, iteration);

                    CommunicationLayer.Send(rootTaskId, cpm, _cancellationSource.Source);

                    _checkpointsWaiting.TryAdd(id, received);
                    retry++;
                }
                while (!received.WaitOne(_timeout) && retry < _retry);

                if (!_checkpoints.TryGetValue(id, out checkpoints))
                {
                    LOGGER.Log(Level.Warning, "Checkpoint not retrieved.");
                    _checkpointsWaiting.TryRemove(id, out received);
                    return false;
                }
            }

            iteration = iteration < 0 ? checkpoints.Keys.Last() : iteration;

            if (!checkpoints.TryGetValue(iteration, out checkpoint))
            {
                LOGGER.Log(Level.Warning, $"Checkpoint for iteration {iteration} not found.");
            }

            return true;
        }

        /// <summary>
        /// Remove a checkpoint.
        /// </summary>
        /// <param name="stageName">The stage of the checkpoint to remove</param>
        /// <param name="operatorId">The operator id of the checkpoint to remove</param>
        public void RemoveCheckpoint(string stageName, int operatorId)
        {
            if (stageName == null || stageName == string.Empty)
            {
                throw new ArgumentException(nameof(stageName), "Null or empty.");
            }

            if (operatorId < 0)
            {
                throw new ArgumentException(nameof(operatorId), "Invalid.");
            }

            var id = new CheckpointIdentifier(stageName, operatorId);
            SortedDictionary<int, ICheckpointState> checkpoints;

            _checkpoints.TryRemove(id, out checkpoints);
        }

        /// <summary>
        /// Dispose the service.
        /// </summary>
        public void Dispose()
        {
            foreach (var waiting in _checkpointsWaiting.Values)
            {
                waiting.Set();
                waiting.Close();
            }
        }

        private void CheckSize(SortedDictionary<int, ICheckpointState> checkpoint)
        {
            if (checkpoint.Keys.Count > _limit)
            {
                var first = checkpoint.Keys.First();
                checkpoint.Remove(first);
            }
        }
    }
}

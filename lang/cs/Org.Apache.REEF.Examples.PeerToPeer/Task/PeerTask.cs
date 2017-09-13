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
using System.Collections.Generic;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Examples.PeerToPeer.Communication;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.PeerToPeer
{
    /// <summary>
    /// A Task that merely prints a greeting and exits.
    /// </summary>
    public sealed class PeerTask : ITask
    {
        private readonly string _taskId;
        private readonly int _taskNumber;
        private readonly string _peerId;
        private readonly Communicator<string> _communicator;
        private readonly Random _rng;
        private bool _done;
        private static readonly string EndMessage = "tschau";
        private readonly Queue<string> _messageQueue;
        private static readonly Logger Logger = Logger.GetLogger(typeof(PeerTask));

        [Inject]
        private PeerTask(
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(PeerConfiguration.NumberOfPeers))] int numberOfFriends,
            Communicator<string> communicator)
        {
            if (!GetTaskId(taskId, out _taskNumber))
            {
                throw new FormatException(string.Format("Could not extract Id from {0}", taskId));
            }

            // Do a circular lookup for the peer Id
            var peerNumber = _taskNumber == numberOfFriends - 1 ? 0 : _taskNumber + 1;
            _peerId = CreateTaskIdFromNumericId(peerNumber);

            // Stash the taskName for later
            _taskId = taskId;

            // Establish communications
            _messageQueue = new Queue<string>();
            _communicator = communicator;
            _communicator.RegisterTask(_messageQueue);

            // Set up the control flow
            _rng = new Random();
            _done = false;
        }

        public byte[] Call(byte[] memento)
        {
            Logger.Log(Level.Info, string.Format("Hello, World! I'm #{0}. I will now communicate with #{1}.", _taskId, _peerId));

            int sentCount = 0;

            // Have the first task initialize the conversation
            if (_taskNumber == 0)
            {
                Logger.Log(Level.Info, string.Format("I'm #{0}. Starting the communication.", _taskId));
                var messageToSend = string.Format("Hi from {0}: {1}", _taskId, sentCount++);
                _communicator.Send(_peerId, messageToSend);
                Logger.Log(Level.Info, string.Format("Sending: {0}", messageToSend));
            }

            while (!IsDone() || _messageQueue.Count > 0)
            {
                while (_messageQueue.Count > 0)
                {
                    var message = _messageQueue.Dequeue();

                    Logger.Log(Level.Info, string.Format("Received a message: \"{0}\"", message));

                    if (IsDone())
                    {
                        continue;
                    }

                    if (message == EndMessage)
                    {
                        Logger.Log(Level.Info, string.Format("I heard {0} and I'm leaving.", EndMessage));
                        Done();
                        continue;
                    }

                    // 20% change of ending the conversation
                    if (_rng.NextDouble() < 0.05)
                    {
                        Logger.Log(Level.Info, "Saying goodbye.");
                        _communicator.Send(_peerId, EndMessage);
                        Done();
                        continue;
                    }

                    var messageToSend = string.Format("Hi from #{0}: {1}", _taskId, sentCount++);
                    _communicator.Send(_peerId, messageToSend);
                    Logger.Log(Level.Info, string.Format("Sending: {0}", messageToSend));
                }
            }

            return null;
        }

        private void Done()
        {
            _done = true;
        }

        private bool IsDone()
        {
            return _done;
        }

        public void Dispose()
        {
            _communicator.Dispose();
        }

        private bool GetTaskId(string taskName, out int taskId)
        {
            if (!int.TryParse(taskName.Substring(taskName.IndexOf('-') + 1), out taskId))
            {
                return false;
            }

            return true;
        }

        private string CreateTaskIdFromNumericId(int taskNumber)
        {
            return string.Format(Constants.TaskIdFormat, taskNumber);
        }
    }
}
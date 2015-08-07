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

using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.IMRUTasks
{
    /// <summary>
    /// Hosts the IMRU UpdateTask in a REEF task
    /// </summary>
    /// <typeparam name="TMapInput">Map input</typeparam>
    /// <typeparam name="TMapOutput">Map output</typeparam>
    /// <typeparam name="TResult">Final result</typeparam>
    public sealed class UpdateTaskHost<TMapInput, TMapOutput, TResult> : ITask
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(UpdateTaskHost<TMapInput, TMapOutput, TResult>));

        private readonly IReduceReceiver<TMapOutput> _dataReceiver;
        private readonly IBroadcastSender<MapInputWithControlMessage<TMapInput>> _dataAndControlMessageSender;
        private readonly IUpdateFunction<TMapInput, TMapOutput, TResult> _updateTask;

        /// <summary>
        /// </summary>
        /// <param name="updateTask">The UpdateTask hosted in this REEF Task.</param>
        /// <param name="groupCommunicationsClient">Used to setup the communications.</param>
        [Inject]
        private UpdateTaskHost(
            IUpdateFunction<TMapInput, TMapOutput, TResult> updateTask,
            IGroupCommClient groupCommunicationsClient)
        {
            _updateTask = updateTask;
            var cg = groupCommunicationsClient.GetCommunicationGroup(IMRUConstants.CommunicationGroupName);
            _dataAndControlMessageSender = cg.GetBroadcastSender<MapInputWithControlMessage<TMapInput>>(IMRUConstants.BroadcastOperatorName);
            _dataReceiver = cg.GetReduceReceiver<TMapOutput>(IMRUConstants.ReduceOperatorName);
        }

        /// <summary>
        /// Performs IMRU iterations on update side
        /// </summary>
        /// <param name="memento"></param>
        /// <returns></returns>
        public byte[] Call(byte[] memento)
        {
            var updateResult = _updateTask.Initialize();
            MapInputWithControlMessage<TMapInput> message =
                new MapInputWithControlMessage<TMapInput>(MapControlMessage.AnotherRound);

            while (updateResult.HasMapInput)
            {
                message.Message = updateResult.MapInput;
                _dataAndControlMessageSender.Send(message);
                updateResult = _updateTask.Update(_dataReceiver.Reduce());
                if (updateResult.HasResult)
                {
                    // TODO[REEF-576]: Emit output somewhere.
                }
            }

            message.ControlMessage = MapControlMessage.Stop;
            _dataAndControlMessageSender.Send(message);

            if (updateResult.HasResult)
            {
                // TODO[REEF-576]: Emit output somewhere.
            }

            return null;
        }

        /// <summary>
        /// Dispose function
        /// </summary>
        public void Dispose()
        {
        }
    }
}
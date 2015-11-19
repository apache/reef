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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
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
        private readonly bool _invokeGC;
        private readonly IIMRUResultHandler<TResult> _resultHandler;

        /// <summary>
        /// </summary>
        /// <param name="updateTask">The UpdateTask hosted in this REEF Task.</param>
        /// <param name="groupCommunicationsClient">Used to setup the communications.</param>
        /// <param name="resultHandler">Result handler</param>
        /// <param name="invokeGC">Whether to call Garbage Collector after each iteration or not</param>
        [Inject]
        private UpdateTaskHost(
            IUpdateFunction<TMapInput, TMapOutput, TResult> updateTask,
            IGroupCommClient groupCommunicationsClient,
            IIMRUResultHandler<TResult> resultHandler,
            [Parameter(typeof(InvokeGC))] bool invokeGC)
        {
            _updateTask = updateTask;
            var cg = groupCommunicationsClient.GetCommunicationGroup(IMRUConstants.CommunicationGroupName);
            _dataAndControlMessageSender =
                cg.GetBroadcastSender<MapInputWithControlMessage<TMapInput>>(IMRUConstants.BroadcastOperatorName);
            _dataReceiver = cg.GetReduceReceiver<TMapOutput>(IMRUConstants.ReduceOperatorName);
            _invokeGC = invokeGC;
            _resultHandler = resultHandler;
        }

        /// <summary>
        /// Performs IMRU iterations on update side
        /// </summary>
        /// <param name="memento"></param>
        /// <returns></returns>
        public byte[] Call(byte[] memento)
        {
            var updateResult = _updateTask.Initialize();
            int iterNo = 0;

            while (updateResult.HasMapInput)
            {
                iterNo++;

                using (
                    var message = new MapInputWithControlMessage<TMapInput>(updateResult.MapInput,
                        MapControlMessage.AnotherRound))
                {
                    _dataAndControlMessageSender.Send(message);
                }

                var input = _dataReceiver.Reduce();

                if (_invokeGC)
                {
                    Logger.Log(Level.Verbose, "Calling Garbage Collector");
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }

                updateResult = _updateTask.Update(input);

                if (updateResult.HasResult)
                {
                    _resultHandler.HandleResult(updateResult.Result);
                }
            }

            MapInputWithControlMessage<TMapInput> stopMessage =
                    new MapInputWithControlMessage<TMapInput>(MapControlMessage.Stop);
            _dataAndControlMessageSender.Send(stopMessage);

            _resultHandler.Dispose();
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
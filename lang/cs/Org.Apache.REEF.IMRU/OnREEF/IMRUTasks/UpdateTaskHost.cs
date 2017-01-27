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
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.IMRUTasks
{
    /// <summary>
    /// Hosts the IMRU UpdateTask in a REEF task
    /// </summary>
    /// <typeparam name="TMapInput">Map input</typeparam>
    /// <typeparam name="TMapOutput">Map output</typeparam>
    /// <typeparam name="TResult">Final result</typeparam>
    [ThreadSafe]
    internal sealed class UpdateTaskHost<TMapInput, TMapOutput, TResult> : TaskHostBase
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(UpdateTaskHost<TMapInput, TMapOutput, TResult>));

        private readonly IReduceReceiver<TMapOutput> _dataReceiver;
        private readonly IBroadcastSender<MapInputWithControlMessage<TMapInput>> _dataAndControlMessageSender;
        private readonly IUpdateFunction<TMapInput, TMapOutput, TResult> _updateTask;
        private readonly IIMRUResultHandler<TResult> _resultHandler;

        /// <summary>
        /// It indicates if the update task has completed and result has been written.
        /// </summary>
        private bool _done;

        /// <summary>
        /// </summary>
        /// <param name="updateTask">The UpdateTask hosted in this REEF Task.</param>
        /// <param name="groupCommunicationsClient">Used to setup the communications.</param>
        /// <param name="resultHandler">Result handler</param>
        /// <param name="taskCloseCoordinator">Task close Coordinator</param>
        /// <param name="invokeGc">Whether to call Garbage Collector after each iteration or not</param>
        /// <param name="taskId">task id</param>
        [Inject]
        private UpdateTaskHost(
            IUpdateFunction<TMapInput, TMapOutput, TResult> updateTask,
            IGroupCommClient groupCommunicationsClient,
            IIMRUResultHandler<TResult> resultHandler,
            TaskCloseCoordinator taskCloseCoordinator,
            [Parameter(typeof(InvokeGC))] bool invokeGc,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId) :
            base(groupCommunicationsClient, taskCloseCoordinator, invokeGc)
        {
            Logger.Log(Level.Info, "Entering constructor of UpdateTaskHost for task id {0}", taskId);
            _updateTask = updateTask;
            _dataAndControlMessageSender =
                _communicationGroupClient.GetBroadcastSender<MapInputWithControlMessage<TMapInput>>(IMRUConstants.BroadcastOperatorName);
            _dataReceiver = _communicationGroupClient.GetReduceReceiver<TMapOutput>(IMRUConstants.ReduceOperatorName);
            _resultHandler = resultHandler;
            Logger.Log(Level.Info, "$$$$_resultHandler." + _resultHandler.GetType().AssemblyQualifiedName);
            Logger.Log(Level.Info, "UpdateTaskHost initialized.");
        }

        /// <summary>
        /// Performs IMRU iterations on update side
        /// </summary>
        /// <returns></returns>
        protected override byte[] TaskBody(byte[] memento)
        {
            UpdateResult<TMapInput, TResult> updateResult = null;
            try
            {
                updateResult = _updateTask.Initialize();
            }
            catch (Exception e)
            {
                HandleTaskAppException(e);
            }

            while (!_cancellationSource.IsCancellationRequested && updateResult.HasMapInput)
            {
                using (
                    var message = new MapInputWithControlMessage<TMapInput>(updateResult.MapInput,
                        MapControlMessage.AnotherRound))
                {
                    _dataAndControlMessageSender.Send(message);
                }

                if (_invokeGc)
                {
                    Logger.Log(Level.Verbose, "Calling Garbage Collector");
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }

                var input = _dataReceiver.Reduce(_cancellationSource);

                try
                {
                    updateResult = _updateTask.Update(input);
                    if (updateResult.HasResult)
                    {
                        _resultHandler.HandleResult(updateResult.Result);
                        _done = true;
                    }
                }
                catch (Exception e)
                {
                    HandleTaskAppException(e);
                }
            }

            if (!_cancellationSource.IsCancellationRequested)
            {
                MapInputWithControlMessage<TMapInput> stopMessage =
                    new MapInputWithControlMessage<TMapInput>(MapControlMessage.Stop);
                _dataAndControlMessageSender.Send(stopMessage);
            }

            if (_done)
            {
                return ByteUtilities.StringToByteArrays(TaskManager.UpdateTaskCompleted);
            }
            return null;
        }

        /// <summary>
        /// Return UpdateHostName
        /// </summary>
        protected override string TaskHostName
        {
            get { return "UpdateTaskHost"; }
        }

        public override void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _resultHandler.Dispose();
                _groupCommunicationsClient.Dispose();
                var disposableTask = _updateTask as IDisposable;
                if (disposableTask != null)
                {
                    disposableTask.Dispose();
                }
            }
        }
    }
}
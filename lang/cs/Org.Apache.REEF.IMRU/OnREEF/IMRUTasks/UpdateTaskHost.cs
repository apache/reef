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
using System.IO;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.IMRU.OnREEF.IMRUTasks
{
    /// <summary>
    /// Hosts the IMRU UpdateTask in a REEF task
    /// </summary>
    /// <typeparam name="TMapInput">Map input</typeparam>
    /// <typeparam name="TMapOutput">Map output</typeparam>
    /// <typeparam name="TResult">Final result</typeparam>
    [ThreadSafe]
    internal sealed class UpdateTaskHost<TMapInput, TMapOutput, TResult> : ITask, IObserver<ICloseEvent>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(UpdateTaskHost<TMapInput, TMapOutput, TResult>));

        private readonly IReduceReceiver<TMapOutput> _dataReceiver;
        private readonly IBroadcastSender<MapInputWithControlMessage<TMapInput>> _dataAndControlMessageSender;
        private readonly IUpdateFunction<TMapInput, TMapOutput, TResult> _updateTask;
        private readonly bool _invokeGC;
        private readonly IIMRUResultHandler<TResult> _resultHandler;

        /// <summary>
        /// Shows if the object has been disposed.
        /// </summary>
        private int _disposed = 0;

        /// <summary>
        /// Group Communication client for the task
        /// </summary>
        private readonly IGroupCommClient _groupCommunicationsClient;

        /// <summary>
        /// Task close Coordinator to handle the work when receiving task close event
        /// </summary>
        private readonly TaskCloseCoordinator _taskCloseCoordinator;

        /// <summary>
        /// The cancellation token to control the group communication operation cancellation
        /// </summary>
        private readonly CancellationTokenSource _cancellationSource;

        /// <summary>
        /// </summary>
        /// <param name="updateTask">The UpdateTask hosted in this REEF Task.</param>
        /// <param name="groupCommunicationsClient">Used to setup the communications.</param>
        /// <param name="resultHandler">Result handler</param>
        /// <param name="taskCloseCoordinator">Task close Coordinator</param>
        /// <param name="invokeGC">Whether to call Garbage Collector after each iteration or not</param>
        [Inject]
        private UpdateTaskHost(
            IUpdateFunction<TMapInput, TMapOutput, TResult> updateTask,
            IGroupCommClient groupCommunicationsClient,
            IIMRUResultHandler<TResult> resultHandler,
            TaskCloseCoordinator taskCloseCoordinator,
            [Parameter(typeof(InvokeGC))] bool invokeGC)
        {
            _updateTask = updateTask;
            _groupCommunicationsClient = groupCommunicationsClient;
            var cg = groupCommunicationsClient.GetCommunicationGroup(IMRUConstants.CommunicationGroupName);
            _dataAndControlMessageSender =
                cg.GetBroadcastSender<MapInputWithControlMessage<TMapInput>>(IMRUConstants.BroadcastOperatorName);
            _dataReceiver = cg.GetReduceReceiver<TMapOutput>(IMRUConstants.ReduceOperatorName);
            _invokeGC = invokeGC;
            _resultHandler = resultHandler;
            _taskCloseCoordinator = taskCloseCoordinator;
            _cancellationSource = new CancellationTokenSource();
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

            while (updateResult.HasMapInput && !_cancellationSource.IsCancellationRequested)
            {
                iterNo++;
                try
                {
                    using (
                        var message = new MapInputWithControlMessage<TMapInput>(updateResult.MapInput,
                            MapControlMessage.AnotherRound))
                    {
                        _dataAndControlMessageSender.Send(message);
                    }

                    var input = _dataReceiver.Reduce(_cancellationSource);

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
                catch (OperationCanceledException e)
                {
                    Logger.Log(Level.Warning, "Received OperationCanceledException in UpdateTaskHost with message: {0}.", e.Message);
                    break;
                }
                catch (IOException e)
                {
                    Logger.Log(Level.Error, "Received IOException in UpdateTaskHost with message: {0}.", e.Message);
                    if (!_cancellationSource.IsCancellationRequested)
                    {
                        throw new IMRUTaskGroupCommunicationException(TaskManager.TaskGroupCommunicationError);
                    }
                    break;
                }
                catch (TcpClientConnectionException e)
                {
                    Logger.Log(Level.Error, "Received TcpClientConnectionException in UpdateTaskHost with message: {0}.", e.Message);
                    if (!_cancellationSource.IsCancellationRequested)
                    {
                        throw new IMRUTaskGroupCommunicationException(TaskManager.TaskGroupCommunicationError);
                    }
                    break;
                }
            }

            if (!_cancellationSource.IsCancellationRequested)
            {
                try
                {
                    MapInputWithControlMessage<TMapInput> stopMessage =
                        new MapInputWithControlMessage<TMapInput>(MapControlMessage.Stop);
                    _dataAndControlMessageSender.Send(stopMessage);
                }
                catch (OperationCanceledException e)
                {
                    Logger.Log(Level.Warning, "Received OperationCanceledException in UpdateTaskHost with message: {0}.", e.Message);
                }
                catch (IOException e)
                {
                    Logger.Log(Level.Error, "Received IOException in UpdateTaskHost with message: {0}.", e.Message);
                    throw new IMRUTaskGroupCommunicationException(TaskManager.TaskGroupCommunicationError);
                }
                catch (TcpClientConnectionException e)
                {
                    Logger.Log(Level.Error, "Received TcpClientConnectionException in UpdateTaskHost with message: {0}.", e.Message);
                    throw new IMRUTaskGroupCommunicationException(TaskManager.TaskGroupCommunicationError);
                }                
            }

            _resultHandler.Dispose();
            _taskCloseCoordinator.SignalTaskStopped();
            Logger.Log(Level.Info, "UpdateTaskHost returned with cancellation token {0}.", _cancellationSource.IsCancellationRequested);
            return null;
        }

        /// <summary>
        /// Task close handler. Call TaskCloseCoordinator to handle the event.
        /// </summary>
        /// <param name="closeEvent"></param>
        public void OnNext(ICloseEvent closeEvent)
        {
            _taskCloseCoordinator.HandleEvent(closeEvent, _cancellationSource);
        }

        /// <summary>
        /// Dispose function. Dispose IGroupCommunicationsClient.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _groupCommunicationsClient.Dispose();
            }
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}
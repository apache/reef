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
using System.Net.Sockets;
using System.Runtime.Remoting;
using System.Threading;
using Org.Apache.REEF.Common.Runtime;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.IMRU.OnREEF.IMRUTasks
{
    internal abstract class TaskHostBase : ITask, IObserver<ICloseEvent>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskHostBase));

        /// <summary>
        /// Shows if the object has been disposed.
        /// </summary>
        protected int _disposed;

        /// <summary>
        /// Group Communication client for the task
        /// </summary>
        protected readonly IGroupCommClient _groupCommunicationsClient;

        /// <summary>
        /// Task close Coordinator to handle the work when receiving task close event
        /// </summary>
        private readonly TaskCloseCoordinator _taskCloseCoordinator;

        /// <summary>
        /// Specify whether to invoke garbage collector or not
        /// </summary>
        protected readonly bool _invokeGc;

        /// <summary>
        /// CommunicationGroupClient for the task
        /// </summary>
        protected readonly ICommunicationGroupClient _communicationGroupClient;

        /// <summary>
        /// The cancellation token to control the group communication operation cancellation
        /// </summary>
        protected readonly CancellationTokenSource _cancellationSource;

        /// <summary>
        /// Machine status for log purpose
        /// </summary>
        private readonly MachineStatus _machineStatus = new MachineStatus();

        /// <summary>
        /// Task host base class to hold the common stuff of both mapper and update tasks
        /// </summary>
        /// <param name="groupCommunicationsClient">Group Communication Client</param>
        /// <param name="taskCloseCoordinator">The class that handles the close event for the task</param>
        /// <param name="invokeGc">specify if want to invoke garbage collector or not </param>
        protected TaskHostBase(
            IGroupCommClient groupCommunicationsClient,
            TaskCloseCoordinator taskCloseCoordinator,
            bool invokeGc)
        {
            Logger.Log(Level.Info, "Entering TaskHostBase constructor with machine status {0}.", _machineStatus.ToString());
            _groupCommunicationsClient = groupCommunicationsClient;
            _communicationGroupClient = groupCommunicationsClient.GetCommunicationGroup(IMRUConstants.CommunicationGroupName);

            _invokeGc = invokeGc;
            _taskCloseCoordinator = taskCloseCoordinator;
            _cancellationSource = new CancellationTokenSource();
        }

        /// <summary>
        /// Handle the exceptions in the Call() method
        /// Default to IMRUSystemException to make it recoverable
        /// </summary>
        public byte[] Call(byte[] memento)
        {
            Logger.Log(Level.Info, "Entering {0} Call() with machine status {1}.", TaskHostName, _machineStatus.ToString());
            try
            {
                _groupCommunicationsClient.Initialize(_cancellationSource);
                return TaskBody(memento);
            }
            catch (Exception e)
            {
                if (e is IMRUTaskAppException)
                {
                    throw;
                }
                if (IsCommunicationException(e))
                {
                    HandleCommunicationException(e);
                }
                else
                {
                    HandleSystemException(e);
                }
            }
            finally
            {
                Logger.Log(Level.Info, "TaskHostBase::Finally");
                _taskCloseCoordinator.SignalTaskStopped();
            }
            Logger.Log(Level.Info, "{0} returned with cancellation token:{1}.", TaskHostName, _cancellationSource.IsCancellationRequested);
            return null;
        }

        private static bool IsCommunicationException(Exception e)
        {
            if (e is OperationCanceledException || e is IOException || e is TcpClientConnectionException ||
                e is RemotingException || e is SocketException ||
                (e is AggregateException && e.InnerException != null && e.InnerException is IOException))
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// The body of Call method. Subclass must override it. 
        /// </summary>
        protected abstract byte[] TaskBody(byte[] memento);

        /// <summary>
        /// Task host name
        /// </summary>
        protected abstract string TaskHostName { get; }

        /// <summary>
        /// Task close handler. Call TaskCloseCoordinator to handle the event.
        /// </summary>
        public void OnNext(ICloseEvent closeEvent)
        {
            _taskCloseCoordinator.HandleEvent(closeEvent, _cancellationSource);
        }

        /// <summary>
        /// Dispose function. Dispose IGroupCommunicationsClient.
        /// </summary>
        public virtual void Dispose()
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

        /// <summary>
        /// Convert the exception into IMRUTaskGroupCommunicationException
        /// </summary>
        protected void HandleCommunicationException(Exception e)
        {
            HandleException(e, new IMRUTaskGroupCommunicationException(TaskManager.TaskGroupCommunicationError, e));
        }

        /// <summary>
        /// Convert the exception into IMRUSystemException
        /// </summary>
        protected void HandleSystemException(Exception e)
        {
            HandleException(e, new IMRUTaskSystemException(TaskManager.TaskSystemError, e));
        }

        /// <summary>
        /// Convert the exception into IMRUTaskAppException
        /// </summary>
        protected void HandleTaskAppException(Exception e)
        {
            HandleException(e, new IMRUTaskAppException(TaskManager.TaskAppError, e));
        }

        /// <summary>
        /// Log and throw target exception if cancellation token is not set
        /// In the cancellation case, simply log and return.
        /// </summary>
        private void HandleException(Exception originalException, Exception targetException)
        {
            Logger.Log(Level.Error,
                "Received exception in {0} with cancellation token {1}: [{2}]",
                TaskHostName,
                _cancellationSource.IsCancellationRequested,
                originalException);
            if (!_cancellationSource.IsCancellationRequested)
            {
                Logger.Log(Level.Error,
                    "{0} is throwing {1} with cancellation token: {2}.",
                    TaskHostName,
                    targetException.GetType(),
                    _cancellationSource.IsCancellationRequested);
                throw targetException;
            }
        }
    }
}
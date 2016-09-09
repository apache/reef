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

namespace Org.Apache.REEF.IMRU.OnREEF.IMRUTasks
{
    /// <summary>
    /// Hosts the IMRU MapTask in a REEF Task.
    /// </summary>
    /// <typeparam name="TMapInput">Map input</typeparam>
    /// <typeparam name="TMapOutput">Map output</typeparam>
    [ThreadSafe]
    internal sealed class MapTaskHost<TMapInput, TMapOutput> : TaskHostBase, ITask, IObserver<ICloseEvent>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MapTaskHost<TMapInput, TMapOutput>));

        private readonly IBroadcastReceiver<MapInputWithControlMessage<TMapInput>> _dataAndMessageReceiver;
        private readonly IReduceSender<TMapOutput> _dataReducer;
        private readonly IMapFunction<TMapInput, TMapOutput> _mapTask;

        /// <summary>
        /// </summary>
        /// <param name="mapTask">The MapTask hosted in this REEF Task.</param>
        /// <param name="groupCommunicationsClient">Used to setup the communications.</param>
        /// <param name="taskCloseCoordinator">Task close Coordinator</param>
        /// <param name="invokeGc">Whether to call Garbage Collector after each iteration or not</param>
        /// <param name="taskId">task id</param>
        [Inject]
        private MapTaskHost(
            IMapFunction<TMapInput, TMapOutput> mapTask,
            IGroupCommClient groupCommunicationsClient,
            TaskCloseCoordinator taskCloseCoordinator,
            [Parameter(typeof(InvokeGC))] bool invokeGc,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId) :
            base(groupCommunicationsClient, taskCloseCoordinator, invokeGc)
        {
            Logger.Log(Level.Info, "Entering constructor of MapTaskHost for task id {0}", taskId);
            _mapTask = mapTask;
            _dataAndMessageReceiver =
                _communicationGroupClient.GetBroadcastReceiver<MapInputWithControlMessage<TMapInput>>(IMRUConstants.BroadcastOperatorName);
            _dataReducer = _communicationGroupClient.GetReduceSender<TMapOutput>(IMRUConstants.ReduceOperatorName);
            Logger.Log(Level.Info, "MapTaskHost initialized.");
        }

        /// <summary>
        /// Performs IMRU iterations on map side
        /// </summary>
        /// <returns></returns>
        protected override byte[] TaskBody(byte[] memento)
        {
            MapControlMessage controlMessage = MapControlMessage.AnotherRound;
            while (!_cancellationSource.IsCancellationRequested && controlMessage != MapControlMessage.Stop)
            {
                if (_invokeGc)
                {
                    Logger.Log(Level.Verbose, "Calling Garbage Collector");
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }

                using (
                    MapInputWithControlMessage<TMapInput> mapInput =
                        _dataAndMessageReceiver.Receive(_cancellationSource))
                {
                    controlMessage = mapInput.ControlMessage;
                    if (controlMessage != MapControlMessage.Stop)
                    {
                        TMapOutput output = default(TMapOutput);
                        try
                        {
                            output = _mapTask.Map(mapInput.Message);
                        }
                        catch (Exception e)
                        {
                            HandleTaskAppException(e);
                        }
                        _dataReducer.Send(output, _cancellationSource);
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Return mapTaskHost name
        /// </summary>
        protected override string TaskHostName
        {
            get { return "MapTaskHost"; }
        }
    }
}
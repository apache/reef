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

using Grpc.Core;
using Org.Apache.REEF.Bridge.Core.Common.Driver;
using Org.Apache.REEF.Bridge.Core.Common.Driver.Events;
using Org.Apache.REEF.Bridge.Core.Proto;
using Org.Apache.REEF.Common.Catalog.Capabilities;
using Org.Apache.REEF.Common.Exceptions;
using Org.Apache.REEF.Common.Runtime;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Void = Org.Apache.REEF.Bridge.Core.Proto.Void;

namespace Org.Apache.REEF.Bridge.Core.Grpc.Driver
{
    internal class DriverClientService : DriverClient.DriverClientBase, IDriverClientService
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DriverClientService));

        private static readonly Void Void = new Void();

        private readonly object _lock = new object();

        private readonly Server _server;

        private readonly int _serverPort;

        private readonly IInjector _injector;

        private readonly BridgeClock _bridgeClock;

        private readonly DriverServiceClient _driverServiceClient;

        private readonly IDictionary<string, BridgeActiveContext> _activeContexts =
            new Dictionary<string, BridgeActiveContext>();

        private readonly IDictionary<string, BridgeRunningTask> _runningTasks =
            new Dictionary<string, BridgeRunningTask>();

        private DriverBridge _driverBridge;

        private bool _receivedStartEvent;

        private bool IsIdle
        {
            get
            {
                lock (_lock)
                {
                    return _receivedStartEvent && DriverBridge.IsIdle && _bridgeClock.IsIdle();
                }
            }
        }

        [Inject]
        private DriverClientService(
            IInjector injector,
            BridgeClock bridgeClock,
            DriverServiceClient driverServiceClient)
        {
            _receivedStartEvent = false;
            _injector = injector;
            _bridgeClock = bridgeClock;
            _driverServiceClient = driverServiceClient;
            _server = new Server
            {
                Services = { DriverClient.BindService(this) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            _server.Start();
            foreach (var serverPort in _server.Ports)
            {
                Logger.Log(Level.Info, "Server port {0}", serverPort.BoundPort);
                _serverPort = serverPort.BoundPort;
            }

            Logger.Log(Level.Info, "Client service started on port {0}", _serverPort);
        }

        #region Driver Client Service implementation

        public void Start()
        {
            try
            {
                // could throw application level exception
                _driverBridge = _injector.GetInstance<DriverBridge>();
                _driverServiceClient.RegisterDriverClientService("localhost", _serverPort);
            }
            catch (Exception ex)
            {
                Logger.Log(Level.Error, "Driver application creation error", ex);
                _driverServiceClient.RegisterDriverClientService(ex);
                _server.ShutdownAsync();
            }
        }

        public void AwaitTermination()
        {
            Logger.Log(Level.Info, "Awaiting client server termination");
            _server.ShutdownTask.Wait();
            Logger.Log(Level.Info, "Client server terminated");
        }

        #endregion Driver Client Service implementation

        public override async Task<Void> AlarmTrigger(AlarmTriggerInfo request, ServerCallContext context)
        {
            await _bridgeClock.OnNextAsync(new BridgeClock.AlarmInfo
            {
                AlarmId = request.AlarmId,
                Timestamp = request.Timestamp
            });
            return Void;
        }

        public override Task<IdleStatus> IdlenessCheckHandler(Void request, ServerCallContext context)
        {
            return Task.FromResult(new IdleStatus
            {
                IsIdle = IsIdle,
                Reason = "Driver client"
            });
        }

        public override async Task<Void> StartHandler(StartTimeInfo request, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Received start event at time {0}", request.StartTime);
                await _driverBridge.DispatchStartEventAsync(new BridgeDriverStarted(new DateTime(request.StartTime)));
                _receivedStartEvent = true;
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public override async Task<ExceptionInfo> StopHandler(StopTimeInfo request, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Received stop event at time {0}", request.StopTime);
                await _driverBridge.DispatchStopEvent(new BridgeDriverStopped(new DateTime(request.StopTime)));
            }
            catch (Exception ex)
            {
                Logger.Log(Level.Error, "Driver stop handler error", ex);
                return GrpcUtils.SerializeException(ex);
            }
            finally
            {
                /* Do not await on shutdown async, which will cause a deadlock since
                 * shutdown waits for this method to return.*/
                _server.ShutdownAsync();
            }
            Logger.Log(Level.Info, "Clean stop handler execution");
            return new ExceptionInfo()
            {
                NoError = true
            };
        }

        public override async Task<Void> ActiveContextHandler(ContextInfo request, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Active context event id {0}", request.ContextId);
                var activeContext = GetOrCreateActiveContext(request);
                await _driverBridge.DispatchActiveContextEvent(activeContext);
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> ClosedContextHandler(ContextInfo request, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Closed context event id {0}", request.ContextId);
                await _driverBridge.DispatchClosedContextEvent(CreateClosedContextAndForget(request));
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> ContextMessageHandler(ContextMessageInfo request, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Context message event id {0}", request.ContextId);
                await _driverBridge.DispatchContextMessageEvent(
                    new BridgeContextMessage(
                        request.ContextId,
                        request.MessageSourceId,
                        request.Payload.ToByteArray()));
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> FailedContextHandler(ContextInfo request, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Failed context event id {0}", request.ContextId);
                BridgeActiveContext activeContext; 
                BridgeActiveContext parentContext = null;
                lock (_lock)
                {
                    if (_activeContexts.TryGetValue(request.ContextId, out activeContext))
                    {
                        _activeContexts.Remove(request.ContextId);
                        parentContext = activeContext.ParentId.IsPresent()
                            ? _activeContexts[activeContext.ParentId.Value]
                            : null;
                    }
                }

                if (activeContext != null)
                {
                    await _driverBridge.DispatchFailedContextEvent(
                        new BridgeFailedContext(
                            activeContext.Id,
                            activeContext.EvaluatorId,
                            CreateEvaluatorDescriptor(request.EvaluatorDescriptorInfo),
                            Optional<IActiveContext>.OfNullable(parentContext)));
                }
                else
                {
                    Logger.Log(Level.Error, "unknown failed context {0}", request.ContextId);
                }
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        // Evaluator handlers

        public override async Task<Void> AllocatedEvaluatorHandler(EvaluatorInfo request, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Allocated evaluator id {0}", request.EvaluatorId);
                var evaluatorDescriptor = CreateEvaluatorDescriptor(request.DescriptorInfo);
                await _driverBridge.DispatchAllocatedEvaluatorEventAsync(
                    new BridgeAllocatedEvaluator(request.EvaluatorId, _driverServiceClient, evaluatorDescriptor));
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> CompletedEvaluatorHandler(EvaluatorInfo request, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Completed evaluator id {0}", request.EvaluatorId);
                await _driverBridge.DispatchCompletedEvaluatorEvent(new BridgeCompletedEvaluator(request.EvaluatorId));
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> FailedEvaluatorHandler(EvaluatorInfo info, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Failed evaluator id {0}", info.EvaluatorId);
                await _driverBridge.DispatchFailedEvaluatorEvent(CreateFailedEvaluator(info));
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        #region Task handlers

        public override async Task<Void> RunningTaskHandler(TaskInfo request, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Running task {0}", request.TaskId);
                var task = GetOrCreateRunningTask(request);
                Logger.Log(Level.Info, "Dispatch running task {0}", task.Id);
                await _driverBridge.DispatchRunningTaskEvent(task);
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> CompletedTaskHandler(TaskInfo request, ServerCallContext context)
        {
            try
            {
                lock (_lock)
                {
                    Logger.Log(Level.Info, "Completed task {0}", request.TaskId);
                    _runningTasks.Remove(request.TaskId);
                }
                var activeContext = GetOrCreateActiveContext(request.Context);
                await _driverBridge.DispatchCompletedTaskEvent(new BridgeCompletedTask(request.Result.ToByteArray(),
                    request.TaskId,
                    activeContext));
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> FailedTaskHandler(TaskInfo request, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Failed task {0}", request.TaskId);
                var failedTask = CreateFailedTaskAndForget(request.TaskId, request.Exception,
                    Optional<IActiveContext>.OfNullable(GetOrCreateActiveContext(request.Context)));
                Logger.Log(Level.Info, "Dispatch failed task {0}", request.TaskId);
                await _driverBridge.DispatchFailedTaskEvent(failedTask);
            }
            catch (Exception ex)
            {
                Logger.Log(Level.Warning, "Failed task exception", ex);
                _bridgeClock.Dispose(ex);
            }
            Logger.Log(Level.Info, "Done with failed task handler");
            return Void;
        }

        public override async Task<Void> TaskMessageHandler(TaskMessageInfo request, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Message from task {0}", request.TaskId);
                await _driverBridge.DispatchTaskMessageEvent(new BridgeTaskMessage(request.Payload.ToByteArray(),
                    request.TaskId,
                    request.MessageSourceId));
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> SuspendedTaskHandler(TaskInfo request, ServerCallContext context)
        {
            try
            {
                Logger.Log(Level.Info, "Suspended task {0}", request.TaskId);
                var activeContext = GetOrCreateActiveContext(request.Context);
                await _driverBridge.DispatchSuspendedTaskEvent(new BridgeSuspendedTask(request.Result.ToByteArray(),
                    request.TaskId,
                    activeContext));
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        #endregion Task handlers

        #region Client handlers

        public override async Task<Void> ClientCloseHandler(Void request, ServerCallContext context)
        {
            try
            {
                await _driverBridge.DispatchClientCloseEvent();
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> ClientCloseWithMessageHandler(ClientMessageInfo request, ServerCallContext context)
        {
            try
            {
                await _driverBridge.DispatchClientCloseWithMessageEvent(request.Payload.ToByteArray());
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> ClientMessageHandler(ClientMessageInfo request, ServerCallContext context)
        {
            try
            {
                await _driverBridge.DispatchClientMessageEvent(request.Payload.ToByteArray());
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        #endregion Client handlers

        #region Driver Restart Handlers

        public override async Task<Void> DriverRestartHandler(DriverRestartInfo request, ServerCallContext context)
        {
            try
            {
                ISet<string> expectedEvaluatorIds = new HashSet<string>(request.ExpectedEvaluatorIds);
                await _driverBridge.DispatchDriverRestartedEvent(new BridgeDriverRestarted(
                    new DateTime(request.StartTime.StartTime),
                    expectedEvaluatorIds,
                    (int)request.ResubmissionAttempts));
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> DriverRestartActiveContextHandler(ContextInfo request, ServerCallContext context)
        {
            try
            {
                var activeContext = GetOrCreateActiveContext(request);
                await _driverBridge.DispatchDriverRestartActiveContextEvent(activeContext);
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> DriverRestartRunningTaskHandler(TaskInfo request, ServerCallContext context)
        {
            try
            {
                var runningTask = GetOrCreateRunningTask(request);
                await _driverBridge.DispatchDriverRestartRunningTaskEvent(runningTask);
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }

            return Void;
        }

        public override async Task<Void> DriverRestartCompletedHandler(DriverRestartCompletedInfo request, ServerCallContext context)
        {
            try
            {
                await _driverBridge.DispatchDriverRestartCompletedEvent(
                    new BridgeDriverRestartCompleted(new DateTime(request.CompletionTime.StopTime),
                        request.IsTimedOut));
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        public override async Task<Void> DriverRestartFailedEvaluatorHandler(EvaluatorInfo request, ServerCallContext context)
        {
            try
            {
                await _driverBridge.DispatchDriverRestartFailedEvaluatorEvent(CreateFailedEvaluator(request));
            }
            catch (Exception ex)
            {
                _bridgeClock.Dispose(ex);
            }
            return Void;
        }

        #endregion Driver Restart Handlers

        #region helper methods

        private BridgeActiveContext GetOrCreateActiveContext(ContextInfo info)
        {
            lock (_lock)
            {
                if (_activeContexts.TryGetValue(info.ContextId, out BridgeActiveContext context))
                {
                    Logger.Log(Level.Verbose, "Context already exists, use it: {0}", info.ContextId);
                    return context;
                }

                Logger.Log(Level.Verbose, "Create active context {0}", info.ContextId);

                context = new BridgeActiveContext(
                    _driverServiceClient,
                    info.ContextId,
                    info.EvaluatorId,
                    Optional<string>.OfNullable(info.ParentId.Length == 0 ? null : info.ParentId),
                    CreateEvaluatorDescriptor(info.EvaluatorDescriptorInfo));

                _activeContexts.Add(info.ContextId, context);

                return context;
            }
        }

        private BridgeRunningTask GetOrCreateRunningTask(TaskInfo task)
        {
            lock (_lock)
            {
                if (_runningTasks.TryGetValue(task.TaskId, out BridgeRunningTask runningTask))
                {
                    return runningTask;
                }
                var activeContext = GetOrCreateActiveContext(task.Context);
                runningTask = new BridgeRunningTask(_driverServiceClient, task.TaskId, activeContext);
                _runningTasks[task.TaskId] = runningTask;
                return runningTask;
            }
        }

        private BridgeFailedTask CreateFailedTaskAndForget(string taskId, ExceptionInfo info, Optional<IActiveContext> context)
        {
            lock (_lock)
            {
                if (_runningTasks.TryGetValue(taskId, out BridgeRunningTask task))
                {
                    Logger.Log(Level.Info, "Create failed task {0}", taskId);
                    _runningTasks.Remove(taskId);
                    return new BridgeFailedTask(
                        Optional<IActiveContext>.Of(task.ActiveContext),
                        task.Id,
                        info.Message,
                        info.Data.ToByteArray());
                }
                else
                {
                    Logger.Log(Level.Warning, "Create unknown failed task {0} with data {1}",
                        taskId, !info.Data.IsEmpty);
                    return new BridgeFailedTask(
                        context,
                        taskId,
                        info.Message,
                        info.Data.ToByteArray());
                }
            }
        }

        private BridgeFailedContext CreateFailedContextAndForget(EvaluatorInfo info, string contextId)
        {
            lock (_lock)
            {
                var evaluatorId = info.EvaluatorId;
                if (_activeContexts.TryGetValue(contextId, out BridgeActiveContext activeContext))
                {
                    _activeContexts.Remove(contextId);
                    var parentContext = activeContext.ParentId.IsPresent()
                        ? _activeContexts[activeContext.ParentId.Value]
                        : null;
                    return new BridgeFailedContext(contextId,
                        evaluatorId,
                        activeContext.EvaluatorDescriptor,
                        Optional<IActiveContext>.OfNullable(parentContext));
                }
                else
                {
                    return new BridgeFailedContext(contextId,
                        evaluatorId,
                        CreateEvaluatorDescriptor(info.DescriptorInfo),
                        Optional<IActiveContext>.Empty());
                }
            }
        }

        private BridgeClosedContext CreateClosedContextAndForget(ContextInfo request)
        {
            lock (_lock)
            {
                if (_activeContexts.TryGetValue(request.ContextId, out BridgeActiveContext activeContext))
                {
                    _activeContexts.Remove(request.ContextId);
                    var parentContext = _activeContexts[activeContext.ParentId.Value];
                    return new BridgeClosedContext(
                        activeContext.Id,
                        activeContext.EvaluatorId,
                        Optional<string>.Of(parentContext.Id),
                        CreateEvaluatorDescriptor(request.EvaluatorDescriptorInfo),
                        parentContext);
                }
                else
                {
                    Logger.Log(Level.Error, "Unknown context {0}", request.ContextId);
                    return new BridgeClosedContext(
                        request.ContextId,
                        request.EvaluatorId,
                        Optional<string>.Empty(),
                        CreateEvaluatorDescriptor(request.EvaluatorDescriptorInfo),
                        null);
                }
            }
        }

        private BridgeFailedEvaluator CreateFailedEvaluator(EvaluatorInfo info)
        {
            try
            {
                var failedContexts = info.Failure.FailedContexts.Select(contextId =>
                    CreateFailedContextAndForget(info, contextId)).Cast<IFailedContext>().ToList();
                var failedTask = string.IsNullOrEmpty(info.Failure.FailedTaskId)
                    ? Optional<IFailedTask>.Empty()
                    : Optional<IFailedTask>.Of(CreateFailedTaskAndForget(info.Failure.FailedTaskId,
                        info.Failure.Exception,
                        Optional<IActiveContext>.Empty()));

                return new BridgeFailedEvaluator(
                    info.EvaluatorId,
                    CreateEvaluatorException(info),
                    failedContexts,
                    failedTask);
            }
            catch (Exception ex)
            {
                Logger.Log(Level.Error, "could not create failed evaluator", ex);
                throw;
            }
        }

        private static EvaluatorException CreateEvaluatorException(EvaluatorInfo eval)
        {
            var failureInfo = eval.Failure;
            var errorBytes = failureInfo.Exception.Data.ToByteArray();
            if (errorBytes == null || errorBytes.Length == 0)
            {
                Logger.Log(Level.Error, "Exception without object details: {0}", failureInfo.Exception.Message);
                return new EvaluatorException(
                    eval.EvaluatorId,
                    failureInfo.Exception.Message,
                    string.Join(";", failureInfo.Exception.StackTrace));
            }
            // When the Exception originates from the C# side.
            Exception inner;
            try
            {
                inner = (Exception)ByteUtilities.DeserializeFromBinaryFormat(errorBytes);
            }
            catch (SerializationException se)
            {
                inner = NonSerializableEvaluatorException.UnableToDeserialize(
                    "Exception from Evaluator was not able to be deserialized, returning a NonSerializableEvaluatorException.",
                    se);
            }
            return new EvaluatorException(eval.EvaluatorId, inner.Message, inner);
        }

        private static IEvaluatorDescriptor CreateEvaluatorDescriptor(EvaluatorDescriptorInfo descriptorInfo)
        {
            var runtimeName = RuntimeName.Local;
            if (!string.IsNullOrWhiteSpace(descriptorInfo.RuntimeName) &&
                !Enum.TryParse(descriptorInfo.RuntimeName, true, out runtimeName))
            {
                throw new ArgumentException($"Unknown runtime name received {descriptorInfo.RuntimeName}");
            }

            if (descriptorInfo.NodeDescriptorInfo == null)
            {
                Logger.Log(Level.Warning, "Node Descriptor not present in Evalautor Descriptor");
                return null;
            }
            var nodeDescriptor = new NodeDescriptor(
                descriptorInfo.NodeDescriptorInfo.IpAddress,
                descriptorInfo.NodeDescriptorInfo.Port,
                descriptorInfo.NodeDescriptorInfo.HostName,
                new CPU(descriptorInfo.Cores),
                new RAM(descriptorInfo.Memory));
            return new EvaluatorDescriptor(nodeDescriptor, descriptorInfo.Memory, descriptorInfo.Cores, runtimeName);
        }

        #endregion helper methods
    }
}
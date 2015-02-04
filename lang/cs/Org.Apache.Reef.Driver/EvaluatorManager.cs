/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using Org.Apache.Reef.Common.Api;
using Org.Apache.Reef.Common.Catalog;
using Org.Apache.Reef.Common.Evaluator;
using Org.Apache.Reef.Common.Exceptions;
using Org.Apache.Reef.Common.ProtoBuf.DriverRuntimeProto;
using Org.Apache.Reef.Common.ProtoBuf.EvaluatorRunTimeProto;
using Org.Apache.Reef.Common.ProtoBuf.ReefServiceProto;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.Driver.Evaluator;
using Org.Apache.Reef.Driver.Task;
using Org.Apache.Reef.Utilities;
using Org.Apache.Reef.Utilities.Diagnostics;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Wake.Remote;
using Org.Apache.Reef.Wake.Time;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

using TaskMessage = Org.Apache.Reef.Tasks.TaskMessage;

namespace Org.Apache.Reef.Driver
{
    /// <summary>
    /// Manages a single Evaluator instance including all lifecycle instances:
    /// (AllocatedEvaluator, CompletedEvaluator, FailedEvaluator).
    /// A (periodic) heartbeat channel is established EvaluatorRuntime -> EvaluatorManager.
    /// The EvaluatorRuntime will (periodically) send (status) messages to the EvaluatorManager using this heartbeat channel.
    /// A (push-based) EventHandler channel is established EvaluatorManager -> EvaluatorRuntime.
    /// The EvaluatorManager uses this to forward Driver messages, launch Tasks, and initiate
    /// control information (e.g., shutdown, suspend).* Manages a single Evaluator instance including all lifecycle instances:
    /// (AllocatedEvaluator, CompletedEvaluator, FailedEvaluator).
    /// A (periodic) heartbeat channel is established EvaluatorRuntime -> EvaluatorManager.
    /// The EvaluatorRuntime will (periodically) send (status) messages to the EvaluatorManager using this heartbeat channel.
    /// A (push-based) EventHandler channel is established EvaluatorManager -> EvaluatorRuntime.
    /// The EvaluatorManager uses this to forward Driver messages, launch Tasks, and initiate control information (e.g., shutdown, suspend).
    /// </summary>
    public class EvaluatorManager : IDisposable, IIdentifiable
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(EvaluatorManager));
        
        private STATE _state = STATE.ALLOCATED;

        private IClock _clock;

        // TODO
        //  private final RemoteManager remoteManager;
        private DriverManager _driverManager;

        private IResourceReleaseHandler _resourceReleaseHandler;

        private IResourceLaunchHandler _resourceLaunchHandler;

        private EvaluatorDescriptorImpl _evaluatorDescriptor;

        private string _evaluatorId;

        private IList<EvaluatorContext> _activeContexts = new List<EvaluatorContext>();

        private HashSet<string> _activeContextIds = new HashSet<string>();

        private IRunningTask _runningTask = null;

        private IObserver<EvaluatorControlProto> _evaluatorControlHandler = null;

        private bool _isResourceReleased = false;

        //TODO
        //private final DispatchingEStage dispatcher;
        private EvaluatorType _type = EvaluatorType.CLR;

        public EvaluatorManager(
            IClock clock,
            //RemoteManager remoteManager,
            DriverManager driverManager,
            IResourceReleaseHandler resourceReleaseHandler,
            IResourceLaunchHandler resourceLaunchHandler,
            //REEFErrorHandler errorHandler,
            string evaluatorId,
            EvaluatorDescriptorImpl evaluatorDescriptor,
            ISet<IObservable<IActiveContext>> activeContextEventHandler,
            ISet<IObservable<IClosedContext>> closedContextEventHandlers,
            ISet<IObservable<FailedContext>> failedContextEventHandlers,
            ISet<IObservable<ContextMessage>> contextMessageHandlers,
            ISet<IObservable<IRunningTask>> runningTaskEventHandlers,
            ISet<IObservable<ICompletedTask>> completedTaskEventHandlers,
            ISet<IObservable<ISuspendedTask>> suspendedTaskEventHandlers,
            ISet<IObservable<TaskMessage>> taskMessageEventHandlers,
            ISet<IObservable<FailedTask>> taskExceptionEventHandlers,
            ISet<IObservable<IAllocatedEvaluator>> allocatedEvaluatorEventHandlers,
            ISet<IObservable<IFailedEvaluator>> failedEvaluatorHandlers,
            ISet<IObservable<ICompletedEvaluator>> completedEvaluatorHandlers)
        {
            _clock = clock;
            //_remoteManager = remoteManager;
            _driverManager = driverManager;
            _resourceReleaseHandler = resourceReleaseHandler;
            _resourceLaunchHandler = resourceLaunchHandler;
            _evaluatorId = evaluatorId;
            _evaluatorDescriptor = evaluatorDescriptor;

            //this.dispatcher = new DispatchingEStage(errorHandler, 16); // 16 threads

            //this.dispatcher.register(ActiveContext.class, activeContextEventHandlers);
            //this.dispatcher.register(ClosedContext.class, closedContextEventHandlers);
            //this.dispatcher.register(FailedContext.class, failedContextEventHandlers);
            //this.dispatcher.register(ContextMessage.class, contextMessageHandlers);

            //this.dispatcher.register(RunningTask.class, runningTaskEventHandlers);
            //this.dispatcher.register(CompletedTask.class, completedTaskEventHandlers);
            //this.dispatcher.register(SuspendedTask.class, suspendedTaskEventHandlers);
            //this.dispatcher.register(TaskMessage.class, taskMessageEventHandlers);
            //this.dispatcher.register(FailedTask.class, taskExceptionEventHandlers);

            //this.dispatcher.register(FailedEvaluator.class, failedEvaluatorHandlers);
            //this.dispatcher.register(CompletedEvaluator.class, completedEvaluatorHandlers);
            //this.dispatcher.register(AllocatedEvaluator.class, allocatedEvaluatorEventHandlers);

            //this.dispatcher.onNext(AllocatedEvaluator.class,
            //    new AllocatedEvaluatorImpl(this, remoteManager.getMyIdentifier()));
        }

        /// <summary>
        /// Various states that the EvaluatorManager could be in. The EvaluatorManager is created when a resource has been allocated by the ResourceManager.
        /// </summary>
        public enum STATE
        {
            ALLOCATED,  // initial state
            SUBMITTED,  // client called AllocatedEvaluator.submitTask() and we're waiting for first contact
            RUNNING,    // first contact received, all communication channels established, Evaluator sent to client.
            DONE,       // clean shutdown
            FAILED,     // some failure occurred.
            KILLED      // unclean shutdown
        }

        public IEvaluatorDescriptor EvaluatorDescriptor
        {
            get
            {
                return _evaluatorDescriptor;
            }
        }

        public INodeDescriptor NodeDescriptor
        {
            get
            {
                return EvaluatorDescriptor.NodeDescriptor;
            }
        }

        public IRunningTask RunningTask
        {
            get
            {
                lock (_evaluatorDescriptor)
                {
                    return _runningTask;
                }
            }
        }

        public string Id
        {
            get
            {
                return _evaluatorId;
            }

            set
            {
            }
        }

        public EvaluatorType Type
        {
            get
            {
                return _type;
            }

            set
            {
                _type = value;
                _evaluatorDescriptor.EvaluatorType = value;
            }
        }

        public void Dispose()
        {
            lock (_evaluatorDescriptor)
            {
                if (_state == STATE.RUNNING)
                {
                    LOGGER.Log(Level.Warning, "Dirty shutdown of running evaluator :" + Id);
                    try
                    {
                        // Killing the evaluator means that it doesn't need to send a confirmation; it just dies.
                        EvaluatorControlProto proto = new EvaluatorControlProto();
                        proto.timestamp = DateTime.Now.Ticks;
                        proto.identifier = Id;
                        proto.kill_evaluator = new KillEvaluatorProto();
                        Handle(proto);
                    }
                    finally
                    {
                        _state = STATE.KILLED;
                    }
                }
            }

            if (!_isResourceReleased)
            {
                try
                {
                    // We need to wait awhile before returning the container to the RM in order to
                    // give the EvaluatorRuntime (and Launcher) time to cleanly exit. 

                    // this.clock.scheduleAlarm(100, new EventHandler<Alarm>() {
                    //@Override
                    //public void onNext(final Alarm alarm) {
                    //  EvaluatorManager.this.resourceReleaseHandler.onNext(
                    //      DriverRuntimeProtocol.ResourceReleaseProto.newBuilder()
                    //          .setIdentifier(EvaluatorManager.this.evaluatorId).build());
                }
                catch (Exception e)
                {
                    Exceptions.Caught(e, Level.Warning, "Force resource release because the client closed the clock.", LOGGER);
                    ResourceReleaseProto proto = new ResourceReleaseProto();
                    proto.identifier = _evaluatorId;
                    _resourceReleaseHandler.OnNext(proto);
                }
                finally
                {
                    _isResourceReleased = true;
                    _driverManager.Release(this);
                }
            }
        }

        /// <summary>
        /// EvaluatorException will trigger is FailedEvaluator and state transition to FAILED
        /// </summary>
        /// <param name="exception"></param>
        public void Handle(EvaluatorException exception)
        {
            lock (_evaluatorDescriptor)
            {
                if (_state >= STATE.DONE)
                {
                    return;
                } 
                LOGGER.Log(Level.Warning, "Failed Evaluator: " + Id + exception.Message);
                try
                {
                    IList<FailedContext> failedContexts = new List<FailedContext>();
                    IList<EvaluatorContext> activeContexts = new List<EvaluatorContext>(_activeContexts);
                    activeContexts = activeContexts.Reverse().ToList();
                    foreach (EvaluatorContext context in activeContexts)
                    {
                        Optional<IActiveContext> parentContext = context.ParentId.IsPresent()
                            ? Optional<IActiveContext>.Of(GetEvaluatorContext(context.ParentId.Value))
                            : Optional<IActiveContext>.Empty();
                        failedContexts.Add(context.GetFailedContext(parentContext, exception));
                    }

                    //Optional<FailedTask> failedTask = _runningTask != null ? 
                    //    Optional<FailedTask>.Of(new FailedTask(_runningTask.Id, exception)) : Optional<FailedTask>.Empty();
                    //LOGGER.Log(Level.Info, "TODO: REPLACE THIS " + failedTask.ToString());
                    //this.dispatcher.onNext(FailedEvaluator.class, new FailedEvaluatorImpl(
                    //exception, failedContextList, failedTaskOptional, this.evaluatorId));
                }
                catch (Exception e)
                {
                    Exceptions.CaughtAndThrow(e, Level.Error, "Exception while handling FailedEvaluator.", LOGGER);
                }
                finally
                {
                    _state = STATE.FAILED; 
                    Dispose();
                }
            }
        }

        public void Handle(IRemoteMessage<EvaluatorHeartbeatProto> evaluatorHearBeatProtoMessage)
        {
            lock (_evaluatorDescriptor)
            {
                EvaluatorHeartbeatProto heartbeatProto = evaluatorHearBeatProtoMessage.Message;
                if (heartbeatProto.evaluator_status != null)
                {
                    EvaluatorStatusProto status = heartbeatProto.evaluator_status;
                    if (status.error != null)
                    {
                        Handle(new EvaluatorException(Id, ByteUtilities.ByteArrarysToString(status.error)));
                        return;
                    }
                    else if (_state == STATE.SUBMITTED)
                    {
                        string evaluatorRId = evaluatorHearBeatProtoMessage.Identifier.ToString();
                        LOGGER.Log(Level.Info, "TODO: REPLACE THIS " + evaluatorRId);
                        // TODO
                        // _evaluatorControlHandler = _remoteManager.getHandler(evaluatorRID, EvaluatorRuntimeProtocol.EvaluatorControlProto.class);
                        _state = STATE.RUNNING;
                        LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Evaluator {0} is running", _evaluatorId));
                    }
                }

                LOGGER.Log(Level.Info, "Evaluator heartbeat: " + heartbeatProto);

                EvaluatorStatusProto evaluatorStatusProto = heartbeatProto.evaluator_status;
                foreach (ContextStatusProto contextStatusProto in heartbeatProto.context_status)
                {
                    Handle(contextStatusProto, heartbeatProto.task_status != null);
                }

                if (heartbeatProto.task_status != null)
                {
                    Handle(heartbeatProto.task_status);
                }

                if (evaluatorStatusProto.state == State.FAILED)
                {
                    _state = STATE.FAILED;
                    EvaluatorException e = evaluatorStatusProto.error != null ?
                        new EvaluatorException(_evaluatorId, ByteUtilities.ByteArrarysToString(evaluatorStatusProto.error)) :
                        new EvaluatorException(_evaluatorId, "unknown cause");
                    LOGGER.Log(Level.Warning, "Failed evaluator: " + Id + e.Message);
                    Handle(e);
                }
                else if (evaluatorStatusProto.state == State.DONE)
                {
                    LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Evaluator {0} done", Id));
                    _state = STATE.DONE;

                  // TODO
                  // dispatcher.onNext(CompletedEvaluator.class, new CompletedEvaluator() {
                  //@Override
                  //public String getId() {
                  //  return EvaluatorManager.this.evaluatorId;
                    Dispose();
                }
            }
            LOGGER.Log(Level.Info, "DONE with evaluator heartbeat");
        }

        public void Handle(ResourceLaunchProto resourceLaunchProto)
        {
            lock (_evaluatorDescriptor)
            {
                if (_state == STATE.ALLOCATED)
                {
                    _state = STATE.SUBMITTED;
                    _resourceLaunchHandler.OnNext(resourceLaunchProto);
                }
                else
                {
                    var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Evaluator manager expected {0} state, but instead is in state {1}", STATE.ALLOCATED, _state));
                    Exceptions.Throw(e, LOGGER);
                }
            }
        }

        /// <summary>
        /// Packages the TaskControlProto in an EvaluatorControlProto and forward it to the EvaluatorRuntime
        /// </summary>
        /// <param name="contextControlProto"></param>
        public void Handle(ContextControlProto contextControlProto)
        {
            lock (_evaluatorDescriptor)
            {
                LOGGER.Log(Level.Info, "Task control message from " + _evaluatorId);
                EvaluatorControlProto evaluatorControlProto = new EvaluatorControlProto();
                evaluatorControlProto.timestamp = DateTime.Now.Ticks;
                evaluatorControlProto.identifier = Id;
                evaluatorControlProto.context_control = contextControlProto;

                Handle(evaluatorControlProto);
            }
        }

        /// <summary>
        /// Forward the EvaluatorControlProto to the EvaluatorRuntime
        /// </summary>
        /// <param name="proto"></param>
        public void Handle(EvaluatorControlProto proto)
        {
            lock (_evaluatorDescriptor)
            {
                if (_state == STATE.RUNNING)
                {
                    _evaluatorControlHandler.OnNext(proto);
                }
                else
                {
                    var e = new InvalidOperationException(
                        string.Format(
                        CultureInfo.InvariantCulture, 
                        "Evaluator manager expects to be in {0} state, but instead is in state {1}", 
                        STATE.RUNNING, 
                        _state));
                    Exceptions.Throw(e, LOGGER);
                }
            }
        }

        /// <summary>
        /// Resource status information from the (actual) resource manager.
        /// </summary>
        /// <param name="resourceStatusProto"></param>
        public void Handle(ResourceStatusProto resourceStatusProto)
        {
            lock (_evaluatorDescriptor)
            {
                State resourceState = resourceStatusProto.state;
                LOGGER.Log(Level.Info, "Resource manager state update: " + resourceState);

                if (resourceState == State.DONE || resourceState == State.FAILED)
                {
                    if (_state < STATE.DONE)
                    {
                        // something is wrong, I think I'm alive but the resource manager runtime says I'm dead
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.Append(
                            string.Format(
                                CultureInfo.InvariantCulture,
                                "The resource manager informed me that Evaluator {0} is in state {1} but I think I am in {2} state",
                                _evaluatorId,
                                resourceState,
                                _state));
                        if (resourceStatusProto.diagnostics != null)
                        {
                            stringBuilder.Append("Cause: " + resourceStatusProto.diagnostics);
                        }
                        if (_runningTask != null)
                        {
                            stringBuilder.Append(
                                string.Format(
                                    CultureInfo.InvariantCulture,
                                    "Taskruntime {0} did not complete before this evaluator died.",
                                    _runningTask.Id));
                        }

                        // RM is telling me its DONE/FAILED - assuming it has already released the resources
                        _isResourceReleased = true;
                        //Handle(new EvaluatorException(_evaluatorId, stringBuilder.ToString(), _runningTask));
                        _state = STATE.KILLED;
                    }
                }
            }
        }

        /// <summary>
        /// Handle a context status update
        /// </summary>
        /// <param name="contextStatusProto"></param>
        /// <param name="notifyClientOnNewActiveContext"></param>
        private void Handle(ContextStatusProto contextStatusProto, bool notifyClientOnNewActiveContext)
        {
            string contextId = contextStatusProto.context_id;
            Optional<string> parentId = contextStatusProto.parent_id != null ?
                Optional<string>.Of(contextStatusProto.parent_id) : Optional<string>.Empty();
            if (ContextStatusProto.State.READY == contextStatusProto.context_state)
            {
                if (!_activeContextIds.Contains(contextId))
                {
                    EvaluatorContext evaluatorContext = new EvaluatorContext(this, contextId, parentId);
                    AddEvaluatorContext(evaluatorContext);
                    if (notifyClientOnNewActiveContext)
                    {
                        LOGGER.Log(Level.Info, "TODO: REPLACE THIS " + evaluatorContext.ToString());
                        //TODO
                        //dispatcher.onNext(ActiveContext.class, context);
                    }
                }
                foreach (ContextStatusProto.ContextMessageProto contextMessageProto in contextStatusProto.context_message)
                {
                    byte[] message = contextMessageProto.message;
                    string sourceId = contextMessageProto.source_id;
                    LOGGER.Log(Level.Info, "TODO: REPLACE THIS " + sourceId + message);
                    //        this.dispatcher.onNext(ContextMessage.class,
                    //new ContextMessageImpl(theMessage, contextID, sourceID));
                }
            }
            else
            {
                if (!_activeContextIds.Contains(contextId))
                {
                    if (ContextStatusProto.State.FAIL == contextStatusProto.context_state)
                    {
                        AddEvaluatorContext(new EvaluatorContext(this, contextId, parentId));
                    }
                    else
                    {
                        var e = new InvalidOperationException("unknown context signaling state " + contextStatusProto.context_state);
                        Exceptions.Throw(e, LOGGER);
                    }
                }
            }

            EvaluatorContext context = GetEvaluatorContext(contextId);
            EvaluatorContext parentContext = context.ParentId.IsPresent() ?
                GetEvaluatorContext(context.ParentId.Value) : null;
            RemoveEvaluatorContext(context);

            if (ContextStatusProto.State.FAIL == contextStatusProto.context_state)
            {
                // TODO
                Exception reason = new InvalidOperationException(ByteUtilities.ByteArrarysToString(contextStatusProto.error));
                Optional<IActiveContext> optionalParentContext = (null == parentContext) ?
                    Optional<IActiveContext>.Empty() : Optional<IActiveContext>.Of(parentContext);
                LOGGER.Log(Level.Info, "TODO: REPLACE THIS " + reason.ToString() + optionalParentContext);
                // TODO
                //this.dispatcher.onNext(FailedContext.class,
                //context.getFailedContext(optionalParentContext, reason));
            }
            else if (ContextStatusProto.State.DONE == contextStatusProto.context_state)
            {
                if (null != parentContext)
                {
                    // TODO
                    //this.dispatcher.onNext(ClosedContext.class, context.getClosedContext(parentContext));
                }
                else
                {
                    LOGGER.Log(Level.Info, "Root context closed. Evaluator closed will trigger final shutdown.");
                }
            }
            else
            {
                var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Unknown context state {0} for context {1}", contextStatusProto.context_state, contextId));
                Exceptions.Throw(e, LOGGER);
            }
        }

        /// <summary>
        /// Handle task status messages.
        /// </summary>
        /// <param name="taskStatusProto"></param>
        private void Handle(TaskStatusProto taskStatusProto)
        {
            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Received task {0} status {1}", taskStatusProto.task_id, taskStatusProto.state));
            string taskId = taskStatusProto.task_id;
            string contextId = taskStatusProto.context_id;
            State taskState = taskStatusProto.state;

            if (taskState == State.INIT)
            {
                EvaluatorContext evaluatorContext = GetEvaluatorContext(contextId);
                _runningTask = new RunningTaskImpl(this, taskId, evaluatorContext);
                // this.dispatcher.onNext(RunningTask.class, this.runningTask);
            }
            else if (taskState == State.SUSPEND)
            {
                EvaluatorContext evaluatorContext = GetEvaluatorContext(contextId);
                _runningTask = null;
                byte[] message = taskStatusProto.result != null ? taskStatusProto.result : null;
                LOGGER.Log(Level.Info, "TODO: REPLACE THIS " + evaluatorContext + message.ToString());
                //this.dispatcher.onNext(SuspendedTask.class, new SuspendedTaskImpl(evaluatorContext, message, taskId));
            }
            else if (taskState == State.DONE)
            {
                EvaluatorContext evaluatorContext = GetEvaluatorContext(contextId);
                _runningTask = null;
                byte[] message = taskStatusProto.result != null ? taskStatusProto.result : null;
                LOGGER.Log(Level.Info, "TODO: REPLACE THIS " + evaluatorContext + message.ToString());
                //this.dispatcher.onNext(CompletedTask.class, new CompletedTaskImpl(evaluatorContext, message, taskId));
            }
            else if (taskState == State.FAILED)
            {
                _runningTask = null;
                //EvaluatorContext evaluatorContext = GetEvaluatorContext(contextId);
                //FailedTask failedTask = taskStatusProto.result != null ?
                //    new FailedTask(taskId, ByteUtilities.ByteArrarysToString(taskStatusProto.result), Optional<IActiveContext>.Of(evaluatorContext)) :
                //    new FailedTask(taskId, "Failed task: " + taskState, Optional<IActiveContext>.Of(evaluatorContext));
                //LOGGER.Log(Level.Info, "TODO: REPLACE THIS " + failedTask.ToString());
                //this.dispatcher.onNext(FailedTask.class, taskException);
            }
            else if (taskStatusProto.task_message.Count > 0)
            {
                if (_runningTask != null)
                {
                    var e = new InvalidOperationException("runningTask must be null when there are multiple task messages");
                    Exceptions.Throw(e, LOGGER);
                }
                foreach (TaskStatusProto.TaskMessageProto taskMessageProto in taskStatusProto.task_message)
                {
                    LOGGER.Log(Level.Info, "TODO: REPLACE THIS " + taskMessageProto.ToString());
                    //        this.dispatcher.onNext(TaskMessage.class,
                    //new TaskMessageImpl(taskMessageProto.getMessage().toByteArray(),
                    //    taskId, contextId, taskMessageProto.getSourceId()));
                }
            }
        }

        private EvaluatorContext GetEvaluatorContext(string id)
        {
            foreach (EvaluatorContext context in _activeContexts)
            {
                if (context.Id.Equals(id))
                {
                    return context;
                }
                var e = new InvalidOperationException("Unknown evaluator context with id " + id);
                Exceptions.Throw(e, LOGGER);
            }
            return null;
        }

        private void AddEvaluatorContext(EvaluatorContext context)
        {
            _activeContexts.Add(context);
            _activeContextIds.Add(context.Id);
        }

        private void RemoveEvaluatorContext(EvaluatorContext context)
        {
            _activeContexts.Remove(context);
            _activeContextIds.Remove(context.Id);
        }

        [NamedParameter(documentation: "The Evaluator Identifier.")]
        public class EvaluatorIdentifier : Name<string>
        {
        }

        [NamedParameter(documentation: "The Evaluator Host.")]
        public class EvaluatorDescriptorName : Name<EvaluatorDescriptorImpl>
        {
        }
    }
}

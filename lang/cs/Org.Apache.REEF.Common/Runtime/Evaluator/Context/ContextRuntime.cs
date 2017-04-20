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
using System.Globalization;
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator.Task;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Context
{
    internal sealed class ContextRuntime : IDisposable
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ContextRuntime));

        /// <summary>
        /// Context-local injector. This contains information that will not be available in child injectors.
        /// </summary>
        private readonly IInjector _contextInjector;

        /// <summary>
        /// Service injector. State in this injector moves to child injectors.
        /// </summary>
        private readonly IInjector _serviceInjector;

        /// <summary>
        /// Convenience class to hold all the event handlers for the context as well as the service instances.
        /// </summary>
        private readonly ContextLifeCycle _contextLifeCycle;

        /// <summary>
        /// The parent context, if any.
        /// </summary>
        private readonly Optional<ContextRuntime> _parentContext;

        /// <summary>
        /// The service objects bound to ServiceConfiguration.
        /// </summary> 
        private readonly ISet<object> _injectedServices;

        /// <summary>
        /// The ContextStart handlers bound to ServiceConfiguration.
        /// </summary>
        private readonly ISet<IObserver<IContextStart>> _serviceContextStartHandlers;

        /// <summary>
        /// The ContextStop handlers bound to ServiceConfiguration.
        /// </summary>
        private readonly ISet<IObserver<IContextStop>> _serviceContextStopHandlers;

        /// <summary>
        /// The TaskStart handlers bound to ServiceConfiguration.
        /// </summary>
        private readonly ISet<IObserver<ITaskStart>> _serviceTaskStartHandlers;

        /// <summary>
        /// The TaskStop handlers bound to ServiceConfiguration.
        /// </summary>
        private readonly ISet<IObserver<ITaskStop>> _serviceTaskStopHandlers;

        /// <summary>
        /// The child context, if any.
        /// </summary>
        private Optional<ContextRuntime> _childContext = Optional<ContextRuntime>.Empty();

        /// <summary>
        /// The currently running task, if any.
        /// </summary>
        private Optional<TaskRuntime> _task = Optional<TaskRuntime>.Empty();

        /// <summary>
        /// Current state of the context.
        /// </summary>
        private ContextStatusProto.State _contextState = ContextStatusProto.State.READY;

        /// <summary>
        /// Create a new ContextRuntime.
        /// </summary>
        /// <param name="serviceInjector"></param>
        /// <param name="contextConfiguration">the Configuration for this context.</param>
        /// <param name="parentContext"></param>
        public ContextRuntime(
                IInjector serviceInjector,
                IConfiguration contextConfiguration,
                Optional<ContextRuntime> parentContext)
        {
            _serviceInjector = serviceInjector;

            // Note that for Service objects and handlers, we are not merging them into a separate
            // class (e.g. ServiceContainer) due to the inability to allow service stacking if an instance 
            // of such a class were to be materialized. i.e. if a ServiceContainer object were initialized
            // and a child ServiceConfiguration is submitted, when the child service injector tries to
            // get the relevant handlers and services set, it will get the same set of handlers as
            // previously instantiated by the parent injector, and thus will not allow the stacking
            // of ServiceConfigurations.
            _injectedServices = serviceInjector.GetNamedInstance<ServicesSet, ISet<object>>();

            _serviceContextStartHandlers = 
                serviceInjector.GetNamedInstance<ContextConfigurationOptions.StartHandlers, ISet<IObserver<IContextStart>>>();

            _serviceContextStopHandlers = 
                serviceInjector.GetNamedInstance<ContextConfigurationOptions.StopHandlers, ISet<IObserver<IContextStop>>>();

            _serviceTaskStartHandlers = 
                serviceInjector.GetNamedInstance<TaskConfigurationOptions.StartHandlers, ISet<IObserver<ITaskStart>>>();

            _serviceTaskStopHandlers = 
                serviceInjector.GetNamedInstance<TaskConfigurationOptions.StopHandlers, ISet<IObserver<ITaskStop>>>();

            _contextInjector = serviceInjector.ForkInjector(contextConfiguration);
            _contextLifeCycle = _contextInjector.GetInstance<ContextLifeCycle>();
            _parentContext = parentContext;

            try
            {
                _contextLifeCycle.Start();
            }
            catch (Exception e)
            {
                const string message = "Encountered Exception in ContextStartHandler.";
                if (ParentContext.IsPresent())
                {
                    throw new ContextStartHandlerException(
                        Id, Optional<string>.Of(ParentContext.Value.Id), message, e);
                }
                
                throw new ContextStartHandlerException(Id, Optional<string>.Empty(), message, e);
            }
        }

        public string Id
        {
            get { return _contextLifeCycle.Id; }
        }

        public Optional<ContextRuntime> ParentContext
        {
            get { return _parentContext; }
        }

        /// <summary>
        /// For testing only!
        /// </summary>
        [Testing]
        internal ISet<object> Services
        {
            get { return _injectedServices; }
        }

        /// <summary>
        /// For testing only!
        /// </summary>
        internal Optional<TaskRuntime> TaskRuntime
        {
            get { return _task; }
        }

        /// <summary>
        /// For testing only!
        /// </summary>
        [Testing]
        internal IInjector ContextInjector
        {
            get
            {
                return _contextInjector;
            }
        }

        /// <summary>
        /// For testing only!
        /// </summary>
        [Testing]
        internal IInjector ServiceInjector 
        {
            get
            {
                return _serviceInjector;
            }
        }

        /// <summary>
        ///  Spawns a new context.
        ///  The new context will have a serviceInjector that is created by forking the one in this object with the given
        ///  serviceConfiguration. The contextConfiguration is used to fork the contextInjector from that new serviceInjector.
        /// </summary>
        /// <param name="childContextConfiguration">the new context's context (local) Configuration.</param>
        /// <param name="childServiceConfiguration">the new context's service Configuration.</param>
        /// <returns>a child context.</returns>
        public ContextRuntime SpawnChildContext(
            IConfiguration childContextConfiguration, 
            IConfiguration childServiceConfiguration = null)
        {
            lock (_contextLifeCycle)
            {
                if (_task.IsPresent())
                {
                    throw new InvalidOperationException(
                        string.Format(
                            CultureInfo.InvariantCulture, 
                            "Attempting to spawn a child context when an Task with id '{0}' is running",
                            _task.Value.TaskId));
                }

                AssertChildContextNotPresent("Attempting to instantiate a child context on a context that is not the topmost active context.");
                
                try
                {
                    var childServiceInjector = childServiceConfiguration == null 
                        ? _serviceInjector.ForkInjector() 
                        : _serviceInjector.ForkInjector(childServiceConfiguration);

                    _childContext = Optional<ContextRuntime>.Of(
                        new ContextRuntime(childServiceInjector, childContextConfiguration, Optional<ContextRuntime>.Of(this)));

                    return _childContext.Value;
                }
                catch (Exception e)
                {
                    LOGGER.Log(Level.Error, "Exception while instantiating the childServiceInjector.", e);
                    var childContextId = GetChildContextId(childContextConfiguration);

                    throw new ContextClientCodeException(childContextId, Optional<string>.Of(Id), "Unable to spawn context", e);
                }
            }
        }

        private static string GetChildContextId(IConfiguration childContextConfiguration)
        {
            var contextId = string.Empty;
            try
            {
                var injector = TangFactory.GetTang().NewInjector(childContextConfiguration);
                contextId = injector.GetNamedInstance<ContextConfigurationOptions.ContextIdentifier, string>();
            }
            catch (InjectionException)
            {
                LOGGER.Log(Level.Error, "Unable to get Context ID from child ContextConfiguration. Using empty string.");
            }

            return contextId;
        }

        /// <summary>
        /// Launches an Task on this context.
        /// </summary>
        /// <param name="taskConfiguration"></param>
        public Thread StartTaskOnNewThread(IConfiguration taskConfiguration)
        {
            lock (_contextLifeCycle)
            {
                LOGGER.Log(Level.Info, "ContextRuntime::StartTask(TaskConfiguration) task is present: " + _task.IsPresent());

                if (_task.IsPresent())
                {
                    LOGGER.Log(Level.Info, "Task state: " + _task.Value.GetTaskState());
                    LOGGER.Log(Level.Info, "ContextRuntime::StartTask(TaskConfiguration) task has ended: " + _task.Value.HasEnded());

                    if (_task.Value.HasEnded())
                    {
                        // clean up state
                        _task = Optional<TaskRuntime>.Empty();
                    }
                    else
                    {
                        // note: java code is putting thread id here
                        throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, 
                            "Attempting to spawn a child context when an Task with id '{0}' is running", _task.Value.TaskId));
                    }
                }

                AssertChildContextNotPresent("Attempting to instantiate a child context on a context that is not the topmost active context.");

                var taskInjector = _contextInjector.ForkInjector(taskConfiguration);

                try
                {
                    var taskRuntime = taskInjector.GetInstance<TaskRuntime>();
                    _task = Optional<TaskRuntime>.Of(taskRuntime);
                    return taskRuntime.StartTaskOnNewThread();
                }
                catch (InjectionException e)
                {
                    var taskId = string.Empty;
                    try
                    {
                        taskId = taskInjector.GetNamedInstance<TaskConfigurationOptions.Identifier, string>();
                    }
                    catch (Exception)
                    {
                        LOGGER.Log(Level.Error, "Unable to get Task ID from TaskConfiguration.");
                    }

                    throw TaskClientCodeException.Create(taskId, Id, "Unable to run the new task", e);
                }
                catch (Exception e)
                {
                    throw new ReefRuntimeException("System error in starting Task.", e);
                }
            }
        }

        /// <summary>
        /// Close this context. If there is a child context, this recursively closes it before closing this context. If
        /// there is an Task currently running, that will be closed.
        /// </summary>
        public void Dispose()
        {
            lock (_contextLifeCycle)
            {
                _contextState = ContextStatusProto.State.DONE;
                if (_task.IsPresent())
                {
                    LOGGER.Log(Level.Warning, "Shutting down an task because the underlying context is being closed.");
                    _task.Value.Close(null);
                }
                if (_childContext.IsPresent())
                {
                    LOGGER.Log(Level.Warning, "Closing a context because its parent context is being closed.");
                    _childContext.Value.Dispose();
                }

                try
                {
                    _contextLifeCycle.Close();
                }
                catch (Exception e)
                {
                    const string message = "Encountered Exception in ContextStopHandler.";
                    if (ParentContext.IsPresent())
                    {
                        throw new ContextStopHandlerException(
                            Id, Optional<string>.Of(ParentContext.Value.Id), message, e);
                    }

                    throw new ContextStopHandlerException(Id, Optional<string>.Empty(), message, e);
                }
                finally
                {
                    if (_parentContext.IsPresent())
                    {
                        ParentContext.Value.ResetChildContext();
                    }

                    foreach (var injectedService in _injectedServices.OfType<IDisposable>())
                    {
                        injectedService.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Issue a suspend call to the Task
        /// Note that due to races, the task might have already ended. In that case, we drop this call and leave a WARNING
        /// in the log.
        /// </summary>
        /// <param name="message"> the suspend message to deliver or null if there is none.</param>
        public void SuspendTask(byte[] message)
        {
            lock (_contextLifeCycle)
            {
                if (!_task.IsPresent())
                {
                    LOGGER.Log(Level.Warning, "Received a suspend task while there was no task running. Ignored");
                    return;
                }
                _task.Value.Suspend(message);
            }
        }

        /// <summary>
        /// Issue a close call to the Task
        /// Note that due to races, the task might have already ended. In that case, we drop this call and leave a WARNING
        /// in the log.
        /// </summary>
        /// <param name="message">the close  message to deliver or null if there is none.</param>
        public void CloseTask(byte[] message)
        {
            lock (_contextLifeCycle)
            {
                if (!_task.IsPresent())
                {
                    LOGGER.Log(Level.Warning, "Received a close task while there was no task running. Ignored");
                    return;
                }
                _task.Value.Close(message);
            }
        }

        /// <summary>
        ///  Deliver a message to the Task
        /// Note that due to races, the task might have already ended. In that case, we drop this call and leave a WARNING
        /// in the log.
        /// </summary>
        /// <param name="message">the message to deliver or null if there is none.</param>
        public void DeliverTaskMessage(byte[] message)
        {
            lock (_contextLifeCycle)
            {
                if (!_task.IsPresent())
                {
                    LOGGER.Log(Level.Warning, "Received an task message while there was no task running. Ignored");
                    return;
                }
                _task.Value.Deliver(message);
            }
        }

        public void HandleContextMessage(byte[] message)
        {
            _contextLifeCycle.HandleContextMessage(message);
        }

        /// <summary>
        /// get state of the running Task
        /// </summary>
        /// <returns> the state of the running Task, if one is running.</returns>
        public Optional<TaskStatusProto> GetTaskStatus()
        {
            lock (_contextLifeCycle)
            {
                if (!_task.IsPresent())
                {
                    return Optional<TaskStatusProto>.Empty();
                }

                if (_task.Value.HasEnded())
                {
                    _task = Optional<TaskRuntime>.Empty();
                    return Optional<TaskStatusProto>.Empty();
                }

                var taskStatusProto = _task.Value.GetStatusProto();
                return Optional<TaskStatusProto>.Of(taskStatusProto);
            }
        }

        /// <summary>
        /// Reset child context when parent is being closed
        /// </summary>
        public void ResetChildContext()
        {
            lock (_contextLifeCycle)
            {
                if (_childContext.IsPresent())
                {
                    _childContext = Optional<ContextRuntime>.Empty();
                    return;
                }

                // To reset a child context, there should always be a child context already present.
                throw new InvalidOperationException("no child context set");
            }
        }

        /// <summary>
        /// get context's status in protocol buffer
        /// </summary>
        /// <returns>this context's status in protocol buffer form.</returns>
        public ContextStatusProto GetContextStatus()
        {
            lock (_contextLifeCycle)
            {
                var contextStatusProto = new ContextStatusProto
                {
                    context_id = Id,
                    context_state = _contextState,
                };
                if (_parentContext.IsPresent())
                {
                    contextStatusProto.parent_id = _parentContext.Value.Id;
                }

                foreach (var source in _contextLifeCycle.ContextMessageSources)
                {
                    // Note: Please do not convert to LINQ expression, as source.Message
                    // may not return the same object in subsequent Get calls.
                    var sourceMessage = source.Message;
                    if (sourceMessage.IsPresent())
                    {
                        var contextMessageProto = new ContextStatusProto.ContextMessageProto
                        {
                            source_id = sourceMessage.Value.MessageSourceId,
                            message = ByteUtilities.CopyBytesFrom(sourceMessage.Value.Bytes),
                        };

                        contextStatusProto.context_message.Add(contextMessageProto);
                    }
                }

                return contextStatusProto;
            }
        }

        /// <summary>
        /// Propagates the IDriverConnection message to the TaskRuntime.
        /// </summary>
        internal void HandleDriverConnectionMessage(IDriverConnectionMessage message)
        {
            lock (_contextLifeCycle)
            {
                if (!_task.IsPresent())
                {
                    LOGGER.Log(Level.Warning, "Received a IDriverConnectionMessage while there was no task running. Ignored");
                    return;
                }

                _task.Value.HandleDriverConnectionMessage(message);
            }
        }

        internal IEnumerable<ContextRuntime> GetContextStack()
        {
            var context = Optional<ContextRuntime>.Of(this);
            while (context.IsPresent())
            {
                yield return context.Value;
                context = context.Value.ParentContext;
            }
        }

        private void AssertChildContextNotPresent(string message)
        {
            if (_childContext.IsPresent())
            {
                throw new InvalidOperationException(message);
            }
        }
    }
}

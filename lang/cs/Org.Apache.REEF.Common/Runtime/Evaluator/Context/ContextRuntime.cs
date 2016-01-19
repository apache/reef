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
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator.Task;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Context
{
    internal sealed class ContextRuntime
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ContextRuntime));

        // Context-local injector. This contains information that will not be available in child injectors.
        private readonly IInjector _contextInjector;

        // Service injector. State in this injector moves to child injectors.
        private readonly IInjector _serviceInjector;

        // Convenience class to hold all the event handlers for the context as well as the service instances.
        private readonly ContextLifeCycle _contextLifeCycle;

        // The parent context, if any.
        private readonly Optional<ContextRuntime> _parentContext;

        // The child context, if any.
        private Optional<ContextRuntime> _childContext = Optional<ContextRuntime>.Empty();

        // The currently running task, if any.
        private Optional<TaskRuntime> _task = Optional<TaskRuntime>.Empty();

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
            var config = contextConfiguration as ContextConfiguration;
            if (config == null)
            {
                Utilities.Diagnostics.Exceptions.Throw(
                    new ArgumentException("contextConfiguration is not of type ContextConfiguration"), LOGGER);
            }
            _contextLifeCycle = new ContextLifeCycle(config.Id);
            _serviceInjector = serviceInjector;
            _parentContext = parentContext;
            try
            {
                _contextInjector = serviceInjector.ForkInjector();
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);

                Optional<string> parentId = ParentContext.IsPresent() ?
                    Optional<string>.Of(ParentContext.Value.Id) :
                    Optional<string>.Empty();
                ContextClientCodeException ex = new ContextClientCodeException(ContextClientCodeException.GetId(contextConfiguration), parentId, "Unable to spawn context", e);
                
                Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }

            // Trigger the context start events on contextInjector.
            _contextLifeCycle.Start();
        }

        /// <summary>
        ///  Create a new ContextRuntime for the root context.
        /// </summary>
        /// <param name="serviceInjector"> </param> the serviceInjector to be used.
        /// <param name="contextConfiguration"> the Configuration for this context.</param>
        public ContextRuntime(
            IInjector serviceInjector,
            IConfiguration contextConfiguration)
            : this(serviceInjector, contextConfiguration, Optional<ContextRuntime>.Empty())
        {
            LOGGER.Log(Level.Info, "Instantiating root context");
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
        ///  Spawns a new context.
        ///  The new context will have a serviceInjector that is created by forking the one in this object with the given
        ///  serviceConfiguration. The contextConfiguration is used to fork the contextInjector from that new serviceInjector.
        /// </summary>
        /// <param name="contextConfiguration">the new context's context (local) Configuration.</param>
        /// <param name="serviceConfiguration">the new context's service Configuration.</param>
        /// <returns>a child context.</returns>
        public ContextRuntime SpawnChildContext(IConfiguration contextConfiguration, IConfiguration serviceConfiguration)
        {
            lock (_contextLifeCycle)
            {
                if (_task.IsPresent())
                {
                    // note: java code is putting thread id here
                    var e = new InvalidOperationException(
                        string.Format(CultureInfo.InvariantCulture, "Attempting to spawn a child context when an Task with id '{0}' is running", _task.Value.TaskId));
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                if (_childContext.IsPresent())
                {
                    var e = new InvalidOperationException("Attempting to instantiate a child context on a context that is not the topmost active context.");
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                try
                {
                    IInjector childServiceInjector = _serviceInjector.ForkInjector(serviceConfiguration);
                    var childContext = new ContextRuntime(childServiceInjector, contextConfiguration, Optional<ContextRuntime>.Of(this));
                    _childContext = Optional<ContextRuntime>.Of(childContext);
                    return childContext;
                }
                catch (Exception e)
                {
                    Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);

                    Optional<string> parentId = ParentContext.IsPresent() ?
                        Optional<string>.Of(ParentContext.Value.Id) :
                        Optional<string>.Empty();
                    ContextClientCodeException ex = new ContextClientCodeException(ContextClientCodeException.GetId(contextConfiguration), parentId, "Unable to spawn context", e);
                    
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
            }
            return null;
        }

        /// <summary>
        /// Spawns a new context without services of its own.
        /// The new context will have a serviceInjector that is created by forking the one in this object. The
        /// contextConfiguration is used to fork the contextInjector from that new serviceInjector.
        /// </summary>
        /// <param name="contextConfiguration">the new context's context (local) Configuration.</param>
        /// <returns> a child context.</returns>
        public ContextRuntime SpawnChildContext(IConfiguration contextConfiguration)
        {
            lock (_contextLifeCycle)
            {
                if (_task.IsPresent())
                {
                    var e = new InvalidOperationException(
                        string.Format(CultureInfo.InvariantCulture, "Attempting to spawn a child context when an Task with id '{0}' is running", _task.Value.TaskId)); // note: java code is putting thread id here
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                if (_childContext.IsPresent())
                {
                    var e = new InvalidOperationException("Attempting to instantiate a child context on a context that is not the topmost active context.");
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                IInjector childServiceInjector = _serviceInjector.ForkInjector();
                ContextRuntime childContext = new ContextRuntime(childServiceInjector, contextConfiguration, Optional<ContextRuntime>.Of(this));
                _childContext = Optional<ContextRuntime>.Of(childContext);
                return childContext;
            }
        }

        /// <summary>
        ///  Launches an Task on this context.
        /// </summary>
        /// <param name="taskConfiguration"></param>
        /// <param name="contextId"></param>
        /// <param name="heartBeatManager"></param>
        public void StartTask(TaskConfiguration taskConfiguration, string contextId, HeartBeatManager heartBeatManager)
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
                        var e = new InvalidOperationException(
                        string.Format(CultureInfo.InvariantCulture, "Attempting to spawn a child context when an Task with id '{0}' is running", _task.Value.TaskId));
                        Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }
                }

                if (_childContext.IsPresent())
                {
                    var e = new InvalidOperationException("Attempting to instantiate a child context on a context that is not the topmost active context.");
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                try
                {
                    IInjector taskInjector = _contextInjector.ForkInjector(taskConfiguration.TangConfig);
                    LOGGER.Log(Level.Info, "Trying to inject task with configuration" + taskConfiguration.ToString());
                    TaskRuntime taskRuntime = new TaskRuntime(taskInjector, contextId, taskConfiguration.TaskId, heartBeatManager);
                    taskRuntime.RunTask();
                    _task = Optional<TaskRuntime>.Of(taskRuntime);
                }
                catch (Exception e)
                {
                    var ex = new TaskClientCodeException(taskConfiguration.TaskId, Id, "Unable to instantiate the new task", e);
                    Utilities.Diagnostics.Exceptions.CaughtAndThrow(ex, Level.Error, "Task start error.", LOGGER);
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
                _contextLifeCycle.Close();
                if (_parentContext.IsPresent())
                {
                    ParentContext.Value.ResetChildContext();
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

        [Obsolete("Deprecated in 0.14, please use HandleContextMessage instead.")]
        public void HandleContextMessaage(byte[] message)
        {
            _contextLifeCycle.HandleContextMessage(message);
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
                if (taskStatusProto.state == State.RUNNING)
                {
                    // only RUNNING status is allowed to rurn here, all other state pushed out to heartbeat 
                    return Optional<TaskStatusProto>.Of(taskStatusProto);
                }

                var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Task state must be RUNNING, but instead is in {0} state", taskStatusProto.state));
                Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                return Optional<TaskStatusProto>.Empty();
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
                }
                Utilities.Diagnostics.Exceptions.Throw(new InvalidOperationException("no child context set"), LOGGER);
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

                foreach (var sourceMessage in _contextLifeCycle.ContextMessageSources.Where(src => src.Message.IsPresent()).Select(src => src.Message.Value))
                {
                    var contextMessageProto = new ContextStatusProto.ContextMessageProto
                    {
                        source_id = sourceMessage.MessageSourceId,
                        message = ByteUtilities.CopyBytesFrom(sourceMessage.Bytes),
                    };
                    contextStatusProto.context_message.Add(contextMessageProto);
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
    }
}

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
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator.Task;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Context
{
    internal sealed class ContextManager : IDisposable
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ContextManager));
        private readonly IHeartBeatManager _heartBeatManager;
        private readonly RootContextLauncher _rootContextLauncher;
        private readonly object _contextLock = new object();
        private readonly AvroConfigurationSerializer _serializer;
        private ContextRuntime _topContext = null;

        [Inject]
        private ContextManager(
            IHeartBeatManager heartBeatManager,
            EvaluatorSettings evaluatorSettings,
            AvroConfigurationSerializer serializer)
        {
            // TODO[JIRA REEF-217]: Inject base Injector and pass Injector to RootContextLauncher
            using (LOGGER.LogFunction("ContextManager::ContextManager"))
            {
                _heartBeatManager = heartBeatManager;
                _serializer = serializer;

                _rootContextLauncher = new RootContextLauncher(
                    evaluatorSettings.RootContextId,
                    evaluatorSettings.RootContextConfig,
                    evaluatorSettings.RootServiceConfiguration,
                    evaluatorSettings.RootTaskConfiguration,
                    heartBeatManager);
            }
        }

        /// <summary>
        /// Start the context manager. This initiates the root context.
        /// </summary>
        public void Start()
        {
            lock (_contextLock)
            {
                _topContext = _rootContextLauncher.GetRootContext();
                LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Instantiating root context with Id {0}", _topContext.Id));

                if (_rootContextLauncher.RootTaskConfig.IsPresent())
                {
                    LOGGER.Log(Level.Info, "Launching the initial Task");
                    try
                    {
                        _topContext.StartTask(_rootContextLauncher.RootTaskConfig.Value, _heartBeatManager);
                    }
                    catch (TaskClientCodeException e)
                    {
                        Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Exception when trying to start a task.", LOGGER);
                        HandleTaskException(e);
                    }
                }
            }
        }

        public bool ContextStackIsEmpty()
        {
            lock (_contextLock)
            {
                return _topContext == null;
            }
        }

        // TODO: codes here are slightly different from java since the protobuf.net does not generate the HasXXX method, may want to switch to proto-port later

        /// <summary>
        /// Processes the given ContextControlProto to launch / close / suspend Tasks and Contexts.
        /// This also triggers the HeartBeatManager to send a heartbeat with the result of this operation.
        /// </summary>
        /// <param name="controlMessage"></param>
        public void HandleTaskControl(ContextControlProto controlMessage)
        {
            try
            {
                byte[] message = controlMessage.task_message;
                if (controlMessage.add_context != null && controlMessage.remove_context != null)
                {
                    Utilities.Diagnostics.Exceptions.Throw(new InvalidOperationException("Received a message with both add and remove context. This is unsupported."), LOGGER);
                }
                if (controlMessage.add_context != null)
                {
                    LOGGER.Log(Level.Info, "AddContext");
                    AddContext(controlMessage.add_context);
                    
                    // support submitContextAndTask()
                    if (controlMessage.start_task != null)
                    {
                        LOGGER.Log(Level.Info, "StartTask");
                        StartTask(controlMessage.start_task);
                    }
                    else
                    {
                        // We need to trigger a heartbeat here. In other cases, the heartbeat will be triggered by the TaskRuntime
                        // Therefore this call can not go into addContext
                        LOGGER.Log(Level.Info, "Trigger Heartbeat");
                        _heartBeatManager.OnNext();
                    }
                }
                else if (controlMessage.remove_context != null)
                {
                    LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "RemoveContext with id {0}", controlMessage.remove_context.context_id));
                    RemoveContext(controlMessage.remove_context.context_id);
                }
                else if (controlMessage.start_task != null)
                {
                    LOGGER.Log(Level.Info, "StartTask only");
                    StartTask(controlMessage.start_task);
                }
                else if (controlMessage.stop_task != null)
                {
                    LOGGER.Log(Level.Info, "CloseTask");
                    lock (_contextLock)
                    {
                        _topContext.CloseTask(message);
                    }
                }
                else if (controlMessage.suspend_task != null)
                {
                    LOGGER.Log(Level.Info, "SuspendTask");
                    lock (_contextLock)
                    {
                        _topContext.SuspendTask(message);
                    }
                }
                else if (controlMessage.task_message != null)
                {
                    LOGGER.Log(Level.Info, "DeliverTaskMessage");
                    lock (_contextLock)
                    {
                        _topContext.DeliverTaskMessage(message);
                    }
                }
                else if (controlMessage.context_message != null)
                {
                    LOGGER.Log(Level.Info, "Handle context control message");
                    ContextMessageProto contextMessageProto = controlMessage.context_message;
                    ContextRuntime context = null;
                    lock (_contextLock)
                    {
                        if (_topContext != null)
                        {
                            context = _topContext.GetContextStack().FirstOrDefault(ctx => ctx.Id.Equals(contextMessageProto.context_id));
                        }
                    }

                    if (context != null)
                    {
                        context.HandleContextMessage(controlMessage.context_message.message);
                    }
                    else
                    {
                        var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Sent message to unknown context {0}", contextMessageProto.context_id));
                        Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }
                }
                else
                {
                    InvalidOperationException e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Unknown task control message: {0}", controlMessage.ToString()));
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                } 
            }
            catch (Exception e)
            {
                if (e is TaskClientCodeException)
                {
                    HandleTaskException(e as TaskClientCodeException);
                }
                else if (e is ContextClientCodeException)
                {
                    HandleContextException(e as ContextClientCodeException);
                }
                Utilities.Diagnostics.Exceptions.CaughtAndThrow(e, Level.Error, LOGGER);
            }  
        }

        /// <summary>
        /// Get TaskStatusProto of the currently running task, if there is any
        /// </summary>
        /// <returns>the TaskStatusProto of the currently running task, if there is any</returns>
        public Optional<TaskStatusProto> GetTaskStatus()
        {
            lock (_contextLock)
            {
                return _topContext == null ? Optional<TaskStatusProto>.Empty() : _topContext.GetTaskStatus();
            }
        }

        /// <summary>
        /// get status of all contexts in the stack.
        /// </summary>
        /// <returns>the status of all contexts in the stack.</returns>
        public ICollection<ContextStatusProto> GetContextStatusCollection()
        {
            ICollection<ContextStatusProto> result = new Collection<ContextStatusProto>();
            lock (_contextLock)
            {
                if (_topContext != null)
                {
                    foreach (var contextStatusProto in _topContext.GetContextStack().Select(context => context.GetContextStatus()))
                    {
                        LOGGER.Log(Level.Verbose, string.Format(CultureInfo.InvariantCulture, "Add context status: {0}", contextStatusProto));
                        result.Add(contextStatusProto);
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Shuts down. This forecefully kills the Task if there is one and then shuts down all Contexts on the stack,
        /// starting at the top.
        /// </summary>
        public void Dispose()
        {
            lock (_contextLock)
            {
                if (_topContext != null)
                {
                    LOGGER.Log(Level.Info, "context stack not empty, forcefully closing context runtime.");
                    _topContext.Dispose();
                    _topContext = null;
                }
            }
        }

        /// <summary>
        /// Propagates the IDriverConnection message to the top level ContextRuntime.
        /// </summary>
        internal void HandleDriverConnectionMessage(IDriverConnectionMessage message)
        {
            lock (_contextLock)
            {
                _topContext.HandleDriverConnectionMessage(message);
            }
        }

        /// <summary>
        /// Add a context to the stack.
        /// </summary>
        /// <param name="addContextProto"></param>
        private void AddContext(AddContextProto addContextProto)
        {
            lock (_contextLock)
            {
                var currentTopContext = _topContext;
                if (!currentTopContext.Id.Equals(addContextProto.parent_context_id, StringComparison.OrdinalIgnoreCase))
                {
                    var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Trying to instantiate a child context on context with id '{0}' while the current top context id is {1}",
                        addContextProto.parent_context_id,
                        currentTopContext.Id));
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }

                var contextConfiguration = _serializer.FromString(addContextProto.context_configuration);

                ContextRuntime newTopContext;
                if (!string.IsNullOrWhiteSpace(addContextProto.service_configuration))
                {
                    var serviceConfiguration = _serializer.FromString(addContextProto.service_configuration);
                    newTopContext = currentTopContext.SpawnChildContext(contextConfiguration, serviceConfiguration);
                }
                else
                {
                    newTopContext = currentTopContext.SpawnChildContext(contextConfiguration);
                }
                _topContext = newTopContext;
            }
        }

        /// <summary>
        /// Remove the context with the given ID from the stack.
        /// </summary>
        /// <param name="contextId"> context id</param>
        private void RemoveContext(string contextId)
        {
            lock (_contextLock)
            {
                string currentTopContextId = _topContext.Id;
                if (!contextId.Equals(_topContext.Id, StringComparison.OrdinalIgnoreCase))
                {
                    var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Trying to close context with id '{0}' while the top context id is {1}", contextId, currentTopContextId));
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                var hasParentContext = _topContext.ParentContext.IsPresent();
                _topContext.Dispose();
                if (hasParentContext)
                {
                    // We did not close the root context. Therefore, we need to inform the
                    // driver explicitly that this context is closed. The root context notification
                    // is implicit in the Evaluator close/done notification.
                    _heartBeatManager.OnNext(); // Ensure Driver gets notified of context DONE state
                }

                // does not matter if null.
                _topContext = _topContext.ParentContext.Value;
            }
            //// System.gc(); // TODO: garbage collect?
        }

        /// <summary>
        /// Launch an Task.
        /// </summary>
        /// <param name="startTaskProto"></param>
        private void StartTask(StartTaskProto startTaskProto)
        {
            lock (_contextLock)
            {
                ContextRuntime currentActiveContext = _topContext;
                string expectedContextId = startTaskProto.context_id;
                if (!expectedContextId.Equals(currentActiveContext.Id, StringComparison.OrdinalIgnoreCase))
                {
                    var e = new InvalidOperationException(
                        string.Format(CultureInfo.InvariantCulture, "Task expected context '{0}' but the active context has Id '{1}'", expectedContextId, currentActiveContext.Id));
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                
                var configuration = _serializer.FromString(startTaskProto.configuration);
                currentActiveContext.StartTask(configuration, _heartBeatManager);
            }
        }

        /// <summary>
        ///  THIS ASSUMES THAT IT IS CALLED ON A THREAD HOLDING THE LOCK ON THE HeartBeatManager
        /// </summary>
        /// <param name="e"></param>
        private void HandleTaskException(TaskClientCodeException e)
        {
            LOGGER.Log(Level.Error, "TaskClientCodeException", e);
            byte[] exception = ByteUtilities.StringToByteArrays(e.ToString());
            TaskStatusProto taskStatus = new TaskStatusProto()
            {
                context_id = e.ContextId,
                task_id = e.TaskId,
                result = exception,
                state = State.FAILED
            };
            LOGGER.Log(Level.Error, "Sending Heartbeat for a failed task: {0}", taskStatus);
            _heartBeatManager.OnNext(taskStatus);
        }

        /// <summary>
        /// THIS ASSUMES THAT IT IS CALLED ON A THREAD HOLDING THE LOCK ON THE HeartBeatManager
        /// </summary>
        /// <param name="e"></param>
        private void HandleContextException(ContextClientCodeException e)
        {
            LOGGER.Log(Level.Error, "ContextClientCodeException", e);
            byte[] exception = ByteUtilities.StringToByteArrays(e.ToString());
            ContextStatusProto contextStatusProto = new ContextStatusProto()
            {
                context_id = e.ContextId,
                context_state = ContextStatusProto.State.FAIL,
                error = exception
            };
            if (e.ParentId.IsPresent())
            {
                contextStatusProto.parent_id = e.ParentId.Value;
            }
            LOGGER.Log(Level.Error, "Sending Heartbeat for a failed context: {0}", contextStatusProto);
            _heartBeatManager.OnNext(contextStatusProto);
        }
    }
}
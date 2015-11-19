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

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator.Task;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Context
{
    public class ContextManager : IDisposable
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ContextManager));
        
        private readonly Stack<ContextRuntime> _contextStack = new Stack<ContextRuntime>();

        private readonly HeartBeatManager _heartBeatManager;

        private readonly RootContextLauncher _rootContextLauncher;

        public ContextManager(HeartBeatManager heartBeatManager, Optional<ServiceConfiguration> rootServiceConfig, Optional<TaskConfiguration> rootTaskConfig)
        {
            using (LOGGER.LogFunction("ContextManager::ContextManager"))
            {
                _heartBeatManager = heartBeatManager;
                _rootContextLauncher = new RootContextLauncher(_heartBeatManager.EvaluatorSettings.RootContextConfig, rootServiceConfig, rootTaskConfig);
            }
        }

        /// <summary>
        /// Start the context manager. This initiates the root context.
        /// </summary>
        public void Start()
        {
            lock (_contextStack)
            {
                ContextRuntime rootContext = _rootContextLauncher.GetRootContext();
                LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Instantiating root context with Id {0}", rootContext.Id));
                _contextStack.Push(rootContext);

                if (_rootContextLauncher.RootTaskConfig.IsPresent())
                {
                    LOGGER.Log(Level.Info, "Launching the initial Task");
                    try
                    {
                        _contextStack.Peek().StartTask(_rootContextLauncher.RootTaskConfig.Value, _rootContextLauncher.RootContextConfig.Id, _heartBeatManager);
                    }
                    catch (TaskClientCodeException e)
                    {
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Exception when trying to start a task.", LOGGER);
                        HandleTaskException(e);
                    }
                }
            }
        }

        public bool ContextStackIsEmpty()
        {
            lock (_contextStack)
            {
                return (_contextStack.Count == 0);
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
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new InvalidOperationException("Received a message with both add and remove context. This is unsupported."), LOGGER);
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
                    _contextStack.Peek().CloseTask(message);
                }
                else if (controlMessage.suspend_task != null)
                {
                    LOGGER.Log(Level.Info, "SuspendTask");
                    _contextStack.Peek().SuspendTask(message);
                }
                else if (controlMessage.task_message != null)
                {
                    LOGGER.Log(Level.Info, "DeliverTaskMessage");
                    _contextStack.Peek().DeliverTaskMessage(message);
                }
                else if (controlMessage.context_message != null)
                {
                    LOGGER.Log(Level.Info, "Handle context contol message");
                    ContextMessageProto contextMessageProto = controlMessage.context_message;
                    bool deliveredMessage = false;
                    foreach (ContextRuntime context in _contextStack)
                    {
                        if (context.Id.Equals(contextMessageProto.context_id))
                        {
                            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Handle context message {0}", controlMessage.context_message.message));
                            context.HandleContextMessaage(controlMessage.context_message.message);
                            deliveredMessage = true;
                            break;
                        }
                    }
                    if (!deliveredMessage)
                    {
                        InvalidOperationException e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Sent message to unknown context {0}", contextMessageProto.context_id));
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }
                }
                else
                {
                    InvalidOperationException e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Unknown task control message: {0}", controlMessage.ToString()));
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                } 
            }
            catch (Exception e)
            {
                if (e is TaskClientCodeException)
                {
                    HandleTaskException((TaskClientCodeException)e);
                }
                else if (e is ContextClientCodeException)
                {
                    HandlContextException((ContextClientCodeException)e);
                }
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.CaughtAndThrow(e, Level.Error, LOGGER);
            }  
        }

        /// <summary>
        /// Get TaskStatusProto of the currently running task, if there is any
        /// </summary>
        /// <returns>the TaskStatusProto of the currently running task, if there is any</returns>
        public Optional<TaskStatusProto> GetTaskStatus()
        {
            if (_contextStack.Count == 0)
            {
                return Optional<TaskStatusProto>.Empty();

                // throw new InvalidOperationException("Asked for an Task status while there isn't even a context running.");
            }
            return _contextStack.Peek().GetTaskStatus();
        }

        /// <summary>
        /// get status of all contexts in the stack.
        /// </summary>
        /// <returns>the status of all contexts in the stack.</returns>
        public ICollection<ContextStatusProto> GetContextStatusCollection()
        {
            ICollection<ContextStatusProto> result = new Collection<ContextStatusProto>();
            foreach (ContextRuntime runtime in _contextStack)
            {
                ContextStatusProto contextStatusProto = runtime.GetContextStatus();
                LOGGER.Log(Level.Verbose, string.Format(CultureInfo.InvariantCulture, "Add context status: {0}", contextStatusProto));
                result.Add(contextStatusProto);
            }
            return result;
        }

        /// <summary>
        /// Shuts down. This forecefully kills the Task if there is one and then shuts down all Contexts on the stack,
        /// starting at the top.
        /// </summary>
        public void Dispose()
        {
            lock (_contextStack)
            {
                if (_contextStack != null && _contextStack.Any())
                {
                    LOGGER.Log(Level.Info, "context stack not empty, forcefully closing context runtime.");
                    ContextRuntime runtime = _contextStack.Last();
                    if (runtime != null)
                    {
                        runtime.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Propagates the IDriverConnection message to the top level ContextRuntime.
        /// </summary>
        internal void HandleDriverConnectionMessage(IDriverConnectionMessage message)
        {
            _contextStack.Peek().HandleDriverConnectionMessage(message);
        }

        /// <summary>
        /// Add a context to the stack.
        /// </summary>
        /// <param name="addContextProto"></param>
        private void AddContext(AddContextProto addContextProto)
        {
            lock (_contextStack)
            {
                ContextRuntime currentTopContext = _contextStack.Peek();
                if (!currentTopContext.Id.Equals(addContextProto.parent_context_id, StringComparison.OrdinalIgnoreCase))
                {
                    var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Trying to instantiate a child context on context with id '{0}' while the current top context id is {1}",
                        addContextProto.parent_context_id,
                        currentTopContext.Id));
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                string contextConfigString = addContextProto.context_configuration;
                ContextConfiguration contextConfiguration = new ContextConfiguration(contextConfigString);
                ContextRuntime newTopContext;
                if (addContextProto.service_configuration != null)
                {
                    ServiceConfiguration serviceConfiguration = new ServiceConfiguration(addContextProto.service_configuration);
                    newTopContext = currentTopContext.SpawnChildContext(contextConfiguration, serviceConfiguration.TangConfig);
                }
                else
                {
                    newTopContext = currentTopContext.SpawnChildContext(contextConfiguration);
                }
                _contextStack.Push(newTopContext);
            }
        }

        /// <summary>
        /// Remove the context with the given ID from the stack.
        /// </summary>
        /// <param name="contextId"> context id</param>
        private void RemoveContext(string contextId)
        {
            lock (_contextStack)
            {
                string currentTopContextId = _contextStack.Peek().Id;
                if (!contextId.Equals(_contextStack.Peek().Id, StringComparison.OrdinalIgnoreCase))
                {
                    var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Trying to close context with id '{0}' while the top context id is {1}", contextId, currentTopContextId));
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                _contextStack.Peek().Dispose();
                if (_contextStack.Count > 1)
                {
                    // We did not close the root context. Therefore, we need to inform the
                    // driver explicitly that this context is closed. The root context notification
                    // is implicit in the Evaluator close/done notification.
                    _heartBeatManager.OnNext(); // Ensure Driver gets notified of context DONE state
                }
                _contextStack.Pop();
            }
            // System.gc(); // TODO: garbage collect?
        }

        /// <summary>
        /// Launch an Task.
        /// </summary>
        /// <param name="startTaskProto"></param>
        private void StartTask(StartTaskProto startTaskProto)
        {
            lock (_contextStack)
            {
                ContextRuntime currentActiveContext = _contextStack.Peek();
                string expectedContextId = startTaskProto.context_id;
                if (!expectedContextId.Equals(currentActiveContext.Id, StringComparison.OrdinalIgnoreCase))
                {
                    var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Task expected context '{0}' but the active context has Id '{1}'", expectedContextId, currentActiveContext.Id));
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                TaskConfiguration taskConfiguration = new TaskConfiguration(startTaskProto.configuration);
                currentActiveContext.StartTask(taskConfiguration, expectedContextId, _heartBeatManager);
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
            LOGGER.Log(Level.Error, string.Format(CultureInfo.InvariantCulture, "Sending Heartbeatb for a failed task: {0}", taskStatus.ToString()));
            _heartBeatManager.OnNext(taskStatus);
        }

        /// <summary>
        /// THIS ASSUMES THAT IT IS CALLED ON A THREAD HOLDING THE LOCK ON THE HeartBeatManager
        /// </summary>
        /// <param name="e"></param>
        private void HandlContextException(ContextClientCodeException e)
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
            LOGGER.Log(Level.Error, string.Format(CultureInfo.InvariantCulture, "Sending Heartbeat for a failed context: {0}", contextStatusProto.ToString()));
            _heartBeatManager.OnNext(contextStatusProto);
        }
    }
}
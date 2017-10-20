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
using System.Globalization;
using System.Runtime.InteropServices;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Bridge.Events;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using ContextMessage = Org.Apache.REEF.Driver.Bridge.Events.ContextMessage;

namespace Org.Apache.REEF.Driver.Bridge
{
    [Private]
    public static class ClrSystemHandlerWrapper
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ClrSystemHandlerWrapper));

        private static DriverBridge _driverBridge;

        public static void Call_ClrSystemAllocatedEvaluatorHandler_OnNext(ulong handle, IAllocatedEvaluatorClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemAllocatedEvaluatorHandler_OnNext", clr2Java.GetId()))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IAllocatedEvaluator> obj = (ClrSystemHandler<IAllocatedEvaluator>)gc.Target;
                obj.OnNext(new AllocatedEvaluator(clr2Java, _driverBridge.ConfigurationStringForProviders));
            }       
        }

        public static void Call_ClrSystemActiveContextHandler_OnNext(ulong handle, IActiveContextClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemActiveContextHandler_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IActiveContext> obj = (ClrSystemHandler<IActiveContext>)gc.Target;
                obj.OnNext(new ActiveContext(clr2Java));
            }            
        }

        public static void Call_ClrSystemDriverRestartActiveContextHandler_OnNext(ulong handle, IActiveContextClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemDriverRestartActiveContextHandler_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IActiveContext> obj = (ClrSystemHandler<IActiveContext>)gc.Target;
                obj.OnNext(new ActiveContext(clr2Java));
            }
        }

        public static void Call_ClrSystemEvaluatorRequestor_OnNext(ulong handle, IEvaluatorRequestorClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemEvaluatorRequestor_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IEvaluatorRequestor> obj = (ClrSystemHandler<IEvaluatorRequestor>)gc.Target;
                obj.OnNext(new EvaluatorRequestor(clr2Java));
            }   
        }

        public static void Call_ClrSystemTaskMessage_OnNext(ulong handle, ITaskMessageClr2Java clr2Java, byte[] message)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemTaskMessage_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<ITaskMessage> obj = (ClrSystemHandler<ITaskMessage>)gc.Target;
                obj.OnNext(new TaskMessage(clr2Java, message));
            }   
        }

        public static void Call_ClrSystemFailedTask_OnNext(ulong handle, IFailedTaskClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemFailedTask_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IFailedTask> obj = (ClrSystemHandler<IFailedTask>)gc.Target;
                obj.OnNext(new FailedTask(clr2Java));
            }
        }

        public static void Call_ClrSystemRunningTask_OnNext(ulong handle, IRunningTaskClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemRunningTask_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IRunningTask> obj = (ClrSystemHandler<IRunningTask>)gc.Target;
                obj.OnNext(new RunningTask(clr2Java));
            }        
        }

        public static void Call_ClrSystemDriverRestartRunningTask_OnNext(ulong handle, IRunningTaskClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemDriverRestartRunningTask_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IRunningTask> obj = (ClrSystemHandler<IRunningTask>)gc.Target;
                obj.OnNext(new RunningTask(clr2Java));
            }
        }

        public static void Call_ClrSystemFailedEvaluator_OnNext(ulong handle, IFailedEvaluatorClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemFailedEvaluator_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IFailedEvaluator> obj = (ClrSystemHandler<IFailedEvaluator>)gc.Target;
                obj.OnNext(new FailedEvaluator(clr2Java));
            }     
        }

        public static void Call_ClrSystemCompletedTask_OnNext(ulong handle, ICompletedTaskClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemCompletedTask_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<ICompletedTask> obj = (ClrSystemHandler<ICompletedTask>)gc.Target;
                obj.OnNext(new CompletedTask(clr2Java));
            }         
        }

        public static void Call_ClrSystemSuspendedTask_OnNext(ulong handle, ISuspendedTaskClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemSuspendedTask_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<ISuspendedTask> obj = (ClrSystemHandler<ISuspendedTask>)gc.Target;
                obj.OnNext(new SuspendedTask(clr2Java));
            }
        }

        public static void Call_ClrSystemCompletedEvaluator_OnNext(ulong handle, ICompletedEvaluatorClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemCompletedEvaluator_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<ICompletedEvaluator> obj = (ClrSystemHandler<ICompletedEvaluator>)gc.Target;
                obj.OnNext(new CompletedEvaluator(clr2Java));
            }
        }

        public static void Call_ClrSystemHttpServer_OnNext(ulong handle, IHttpServerBridgeClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemHttpServer_OnNext"))
            {
                try
                {
                    GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                    if (!gc.IsAllocated)
                    {
                        LOGGER.Log(Level.Warning, "gc is not allocated.");
                    } 
                    ClrSystemHandler<IHttpMessage> obj = (ClrSystemHandler<IHttpMessage>)gc.Target;
                    obj.OnNext(new HttpMessage(clr2Java));
                }
                catch (Exception ex)
                {
                    LOGGER.Log(Level.Info, "Caught exception: " + ex.Message + ex.StackTrace);
                    Exceptions.CaughtAndThrow(ex, Level.Warning,  LOGGER);
                }
            }
        }

        /// <summary>
        /// Invokes event handlers registered to the closed context event.
        /// </summary>
        /// <param name="handle">Pointer to the event handler object</param>
        /// <param name="clr2Java">Proxy object to the Java closed context.</param>
        public static void Call_ClrSystemClosedContext_OnNext(ulong handle, IClosedContextClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemClosedContext_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IClosedContext> obj = (ClrSystemHandler<IClosedContext>)gc.Target;
                obj.OnNext(new ClosedContext(clr2Java));
            }
        }

        public static void Call_ClrSystemFailedContext_OnNext(ulong handle, IFailedContextClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemFailedContext_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IFailedContext> obj = (ClrSystemHandler<IFailedContext>)gc.Target;
                obj.OnNext(new FailedContext(clr2Java));
            }
        }

        public static void Call_ClrSystemContextMessage_OnNext(ulong handle, IContextMessageClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemContextMessage_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IContextMessage> obj = (ClrSystemHandler<IContextMessage>)gc.Target;
                obj.OnNext(new ContextMessage(clr2Java));
            }
        }

        public static void Call_ClrSystemDriverRestartCompleted_OnNext(ulong handle, IDriverRestartCompletedClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemDriverRestartCompleted_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IDriverRestartCompleted> obj = (ClrSystemHandler<IDriverRestartCompleted>)gc.Target;
                obj.OnNext(new DriverRestartCompleted(clr2Java));
            }
        }

        public static void Call_ClrSystemDriverRestartFailedEvaluator_OnNext(ulong handle, IFailedEvaluatorClr2Java clr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemDriverRestartFailedEvaluator_OnNext"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                ClrSystemHandler<IFailedEvaluator> obj = (ClrSystemHandler<IFailedEvaluator>)gc.Target;
                obj.OnNext(new FailedEvaluator(clr2Java));
            }
        }

        public static float Call_ProgressProvider_GetProgress(ulong handle)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ProgressProvider_GetProgress"))
            {
                GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
                IProgressProvider obj = (IProgressProvider)gc.Target;
                return obj.Progress;
            }
        }

        /// <summary>
        /// Invokes event handlers registered to the driver start event.
        /// </summary>
        /// <param name="startTime"><see cref="DateTime"/> object that represents when this method was called.</param>
        public static void Call_ClrSystemStartHandler_OnStart(DateTime startTime)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemStartHandler_OnStart"))
            {
                LOGGER.Log(Level.Info, "*** Start time is " + startTime);
                _driverBridge.StartHandlersOnNext(startTime);
            }
        }

        /// <summary>
        /// Invokes event handlers registered to the driver restart event.
        /// </summary>
        /// <param name="driverRestartedClr2Java">Proxy object to the Java driver restart event object.</param>
        public static void Call_ClrSystemRestartHandler_OnRestart(IDriverRestartedClr2Java driverRestartedClr2Java)
        {
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystemRestartHandler_OnRestart"))
            {
                LOGGER.Log(Level.Info, "*** Restart time is " + driverRestartedClr2Java.GetStartTime());
                _driverBridge.RestartHandlerOnNext(driverRestartedClr2Java);
            }
        }

        /// <summary>
        /// Configure and return a manager object holding all subscriptions given to REEF events on the .NET side.
        /// </summary>
        /// <param name="httpServerPort">String representation of the http port of the Java-side driver.</param>
        /// <param name="evaluatorRequestorClr2Java">Proxy object to the Java evaluator requestor object.</param>
        /// <returns><see cref="BridgeHandlerManager"/> object that contains .NET handles for each REEF event.</returns>
        public static BridgeHandlerManager Call_ClrSystem_SetupBridgeHandlerManager(
            string httpServerPort,
            IEvaluatorRequestorClr2Java evaluatorRequestorClr2Java)
        {
            IEvaluatorRequestor evaluatorRequestor = new EvaluatorRequestor(evaluatorRequestorClr2Java);
            using (LOGGER.LogFunction("ClrSystemHandlerWrapper::Call_ClrSystem_SetupBridgeHandlerManager"))
            {
                LOGGER.Log(Level.Info, "*** httpServerPort: " + httpServerPort);
                return GetHandlers(httpServerPort, evaluatorRequestor);
            }
        }

        private static BridgeHandlerManager GetHandlers(string httpServerPortNumber, IEvaluatorRequestor evaluatorRequestor)
        {
            var injector = BridgeConfigurationProvider.GetBridgeInjector(evaluatorRequestor);

            try
            {
                var port = injector.GetInstance<HttpServerPort>();
                port.PortNumber = httpServerPortNumber == null
                    ? 0
                    : int.Parse(httpServerPortNumber, CultureInfo.InvariantCulture);

                _driverBridge = injector.GetInstance<DriverBridge>();
            }
            catch (Exception e)
            {
                Exceptions.CaughtAndThrow(e, Level.Error, "Cannot get instance.", LOGGER);
            }

            return _driverBridge.Subscribe();
        }
    }
}
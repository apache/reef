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
using System.Configuration;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.Reef.Common;
using Org.Apache.Reef.Common.Context;
using Org.Apache.Reef.Common.Evaluator.Context;
using Org.Apache.Reef.Common.ProtoBuf.ReefProtocol;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Services;
using Org.Apache.Reef.Tasks;
using Org.Apache.Reef.Utilities;
using Org.Apache.Reef.Utilities.Diagnostics;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Formats;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Wake.Remote;
using Org.Apache.Reef.Wake.Remote.Impl;
using Org.Apache.Reef.Wake.Time.Runtime.Event;

namespace Org.Apache.Reef.Evaluator
{
    public class Evaluator
    {
        private static Logger _logger;

        private static int _heartbeatPeriodInMs = Constants.DefaultEvaluatorHeartbeatPeriodInMs;

        private static int _heartbeatMaxRetry = Constants.DefaultEvaluatorHeartbeatMaxRetry;

        private static IInjector _injector;

        private static EvaluatorConfigurations _evaluatorConfig;

        public static void Main(string[] args)
        {
            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "START: {0} Evaluator::InitInjector.", DateTime.Now));
            Stopwatch timer = new Stopwatch();
            InitInjector();
            SetCustomTraceListners();
            timer.Stop();
            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "EXIT: {0} Evaluator::InitInjector. Duration: [{1}].", DateTime.Now, timer.Elapsed));

            RuntimeClock clock;

            using (_logger.LogScope("Evaluator::Main"))
            {
                string debugEnabledString = Environment.GetEnvironmentVariable("Org.Apache.Reef.EvaluatorDebug");
                if (!string.IsNullOrWhiteSpace(debugEnabledString) &&
                    debugEnabledString.Equals("enabled", StringComparison.OrdinalIgnoreCase))
                {
                    while (true)
                    {
                        if (Debugger.IsAttached)
                        {
                            break;
                        }
                        else
                        {
                            _logger.Log(Level.Info, "Evaluator in debug mode, waiting for debugger to be attached...");
                            Thread.Sleep(2000);
                        }
                    }
                }

                AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;

                string heartbeatPeriodFromConfig = ConfigurationManager.AppSettings["EvaluatorHeartbeatPeriodInMs"];

                int heartbeatPeriod = 0;

                if (!string.IsNullOrWhiteSpace(heartbeatPeriodFromConfig) &&
                    int.TryParse(heartbeatPeriodFromConfig, out heartbeatPeriod))
                {
                    _heartbeatPeriodInMs = heartbeatPeriod;
                }
                _logger.Log(Level.Verbose,
                            "Evaluator heartbeat period set to be " + _heartbeatPeriodInMs + " milliSeconds.");

                int maxHeartbeatRetry = 0;
                string heartbeatMaxRetryFromConfig = ConfigurationManager.AppSettings["EvaluatorHeartbeatRetryMaxTimes"];

                if (!string.IsNullOrWhiteSpace(heartbeatMaxRetryFromConfig) &&
                    int.TryParse(heartbeatMaxRetryFromConfig, out maxHeartbeatRetry))
                {
                    _heartbeatMaxRetry = maxHeartbeatRetry;
                }
                _logger.Log(Level.Verbose, "Evaluator heatrbeat max retry set to be " + _heartbeatMaxRetry + " times.");

                if (args.Count() < 2)
                {
                    var e = new InvalidOperationException("must supply at least the rId and evaluator config file");
                    Exceptions.Throw(e, _logger);
                }

                // remote driver Id
                string rId = args[0];

                // evaluator configuraiton file
                string evaluatorConfigurationPath = args[1];

                ICodec<REEFMessage> reefMessageCodec = new REEFMessageCodec();

                _evaluatorConfig = new EvaluatorConfigurations(evaluatorConfigurationPath);

                string rootContextConfigString = _evaluatorConfig.RootContextConfiguration;
                if (string.IsNullOrWhiteSpace(rootContextConfigString))
                {
                    Exceptions.Throw(new ArgumentException("empty or null rootContextConfigString"), _logger);
                }
                ContextConfiguration rootContextConfiguration = new ContextConfiguration(rootContextConfigString);

                string taskConfig = _evaluatorConfig.TaskConfiguration;
                Optional<TaskConfiguration> rootTaskConfig = string.IsNullOrEmpty(taskConfig)
                                        ? Optional<TaskConfiguration>.Empty()
                                        : Optional<TaskConfiguration>.Of(
                                            new TaskConfiguration(taskConfig));
                string rootServiceConfigString = _evaluatorConfig.RootServiceConfiguration;
                Optional<ServiceConfiguration> rootServiceConfig = string.IsNullOrEmpty(rootServiceConfigString)
                                        ? Optional<ServiceConfiguration>.Empty()
                                        : Optional<ServiceConfiguration>.Of(
                                            new ServiceConfiguration(
                                                rootServiceConfigString));
 
                // remoteManager used as client-only in evaluator
                IRemoteManager<REEFMessage> remoteManager = new DefaultRemoteManager<REEFMessage>(reefMessageCodec);
                IRemoteIdentifier remoteId = new SocketRemoteIdentifier(NetUtilities.ParseIpEndpoint(rId));

                ConfigurationModule module = new ConfigurationModuleBuilder().Build();
                IConfiguration clockConfiguraiton = module.Build();

                clock =
                    TangFactory.GetTang().NewInjector(clockConfiguraiton).GetInstance<RuntimeClock>();
                    _logger.Log(Level.Info, "Application Id: " + _evaluatorConfig.ApplicationId);

                EvaluatorSettings evaluatorSettings = new EvaluatorSettings(
                    _evaluatorConfig.ApplicationId,
                    _evaluatorConfig.EvaluatorId,
                    _heartbeatPeriodInMs,
                    _heartbeatMaxRetry,
                    rootContextConfiguration,
                    clock,
                    remoteManager,
                    _injector);

                HeartBeatManager heartBeatManager = new HeartBeatManager(evaluatorSettings, remoteId);
                ContextManager contextManager = new ContextManager(heartBeatManager, rootServiceConfig, rootTaskConfig);
                EvaluatorRuntime evaluatorRuntime = new EvaluatorRuntime(contextManager, heartBeatManager);

                // TODO: repalce with injectionFuture
                heartBeatManager._evaluatorRuntime = evaluatorRuntime;
                heartBeatManager._contextManager = contextManager;

                SetRuntimeHanlders(evaluatorRuntime, clock);
            }

            Task evaluatorTask = Task.Run(new Action(clock.Run));
            evaluatorTask.Wait();            
        }

        private static void InitInjector()
        {
            string clrRuntimeConfigurationFile = Path.Combine(Directory.GetCurrentDirectory(), "reef", "global",
                                                                Common.Constants.ClrBridgeRuntimeConfiguration);
            if (!File.Exists(clrRuntimeConfigurationFile))
            {
                var e =
                    new InvalidOperationException("Cannot find clrRuntimeConfiguration from " +
                                                    clrRuntimeConfigurationFile);
                Exceptions.Throw(e, _logger);
            }

            try
            {
                IConfiguration clrBridgeConfiguration =
                    new AvroConfigurationSerializer().FromFile(clrRuntimeConfigurationFile);
                _injector = TangFactory.GetTang().NewInjector(clrBridgeConfiguration);
            }
            catch (Exception e)
            {
                Exceptions.Caught(e, Level.Error, "Cannot obtain injector from clr bridge configuration.", _logger);
                Exceptions.Throw(
                    new InvalidOperationException("Cannot obtain injector from clr bridge configuration.", e),
                    _logger);
            }
        }

        private static void SetCustomTraceListners()
        {
            ISet<TraceListener> customTraceListeners;
            CustomTraceListeners listeners = null;
            try
            {
                listeners = _injector.GetInstance<CustomTraceListeners>();
                customTraceListeners = listeners.Listeners;
            }
            catch (Exception e)
            {
                Exceptions.Caught(e, Level.Error, _logger);
                // custom trace listner not set properly, use empty set
                customTraceListeners = new HashSet<TraceListener>();
            }
            foreach (TraceListener listener in customTraceListeners)
            {
                Logger.AddTraceListner(listener);
            }
            _logger = Logger.GetLogger(typeof(Evaluator));
            CustomTraceLevel traceLevel = _injector.GetInstance<CustomTraceLevel>();
            Logger.SetCustomLevel(traceLevel.TraceLevel);
        }

        private static void UnhandledExceptionHandler(object sender, UnhandledExceptionEventArgs e)
        {
            Exception ex = default(Exception);
            ex = (Exception)e.ExceptionObject;
            _logger.Log(Level.Error, "Unhandled exception caught in Evaluator.", ex);
            Exceptions.Throw(new InvalidOperationException("Unhandled exception caught in Evaluator.", ex), _logger);
        }

        // set the handlers for runtimeclock manually
        // we only need runtimestart and runtimestop handlers now
        private static void SetRuntimeHanlders(EvaluatorRuntime evaluatorRuntime, RuntimeClock clock)
        {
            HashSet<IObserver<RuntimeStart>> runtimeStarts = new HashSet<IObserver<RuntimeStart>>();
            runtimeStarts.Add(evaluatorRuntime);
            InjectionFutureImpl<ISet<IObserver<RuntimeStart>>> injectRuntimeStart = new InjectionFutureImpl<ISet<IObserver<RuntimeStart>>>(runtimeStarts);
            clock.InjectedRuntimeStartHandler = injectRuntimeStart;

            HashSet<IObserver<RuntimeStop>> runtimeStops = new HashSet<IObserver<RuntimeStop>>();
            runtimeStops.Add(evaluatorRuntime);
            InjectionFutureImpl<ISet<IObserver<RuntimeStop>>> injectRuntimeStop = new InjectionFutureImpl<ISet<IObserver<RuntimeStop>>>(runtimeStops);
            clock.InjectedRuntimeStopHandler = injectRuntimeStop;
        }
    }
}
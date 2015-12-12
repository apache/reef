/*
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
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Common.Runtime.Evaluator;
using Org.Apache.REEF.Common.Runtime.Evaluator.Utils;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Evaluator.Exceptions;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Time.Runtime;
using Org.Apache.REEF.Wake.Time.Runtime.Event;

namespace Org.Apache.REEF.Evaluator
{
    public sealed class Evaluator
    {
        private static Logger logger = Logger.GetLogger(typeof(Evaluator));
        private readonly RuntimeClock _clock;

        [Inject]
        private Evaluator(
            RuntimeClock clock,
            EvaluatorRuntime evaluatorRuntime,
            CustomTraceListeners customTraceListeners,
            CustomTraceLevel customTraceLevel)
        {
            _clock = clock;
            SetCustomTraceListeners(customTraceListeners, customTraceLevel);
            SetRuntimeHandlers(evaluatorRuntime, clock);
        }

        private void Run()
        {
            _clock.Run();
        }

        /// <summary>
        /// The command line to run it is "evaluator.exe evaluator.config"
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args)
        {
            try
            {
                if (args.Count() != 1)
                {
                    var e = new InvalidOperationException("Must supply only the evaluator.config file!");
                    Utilities.Diagnostics.Exceptions.Throw(e, logger);
                }

                if (IsDebuggingEnabled())
                {
                    AttachDebugger();
                }
                AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;

                Evaluator evaluator = TangFactory.GetTang()
                    .NewInjector(ReadClrBridgeConfiguration(), ReadEvaluatorConfiguration(args[0]))
                    .GetInstance<Evaluator>();

                evaluator.Run();
                logger.Log(Level.Info, "Evaluator is returned from Run()");
            }
            catch (Exception e)
            {
                Fail(e);
            }
        }

        /// <summary>
        /// Determines whether debugging is enabled.
        /// </summary>
        /// <returns>true, if debugging is enabled</returns>
        private static bool IsDebuggingEnabled()
        {
            var debugEnabledString = Environment.GetEnvironmentVariable("Org.Apache.REEF.EvaluatorDebug");
            return !string.IsNullOrWhiteSpace(debugEnabledString) &&
                   debugEnabledString.Equals("enabled", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Waits for the debugger to be attached.
        /// </summary>
        private static void AttachDebugger()
        {
            // Wait for the debugger
            while (true)
            {
                if (Debugger.IsAttached)
                {
                    break;
                }

                logger.Log(Level.Info, "Evaluator in debug mode, waiting for debugger to be attached...");
                Thread.Sleep(2000);
            }
        }

        /// <summary>
        /// Reads the Evaluator Configuration.
        /// </summary>
        /// <exception cref="EvaluatorConfigurationFileNotFoundException">When the configuration file cannot be found.</exception>
        /// <exception cref="EvaluatorConfigurationParseException">When the configuration file exists, but can't be deserialized.</exception>
        /// <returns></returns>
        private static IConfiguration ReadClrBridgeConfiguration()
        {
            var clrRuntimeConfigurationFile = Path.Combine(Directory.GetCurrentDirectory(), "reef", "global",
                new REEFFileNames().GetClrBridgeConfigurationName());

            if (!File.Exists(clrRuntimeConfigurationFile))
            {
                throw new EvaluatorConfigurationFileNotFoundException(clrRuntimeConfigurationFile);
            }
            
            try
            {
                var clrDriverConfig = new AvroConfigurationSerializer().FromFile(clrRuntimeConfigurationFile);
                logger.Log(Level.Info, 
                    string.Format(CultureInfo.CurrentCulture, "Clr Driver Configuration is deserialized from file {0}:", clrRuntimeConfigurationFile));
                return clrDriverConfig;
            }
            catch (Exception e)
            {
                throw new EvaluatorConfigurationParseException(e);
            }
        }

        private static IConfiguration ReadEvaluatorConfiguration(string evaluatorConfigFile)
        {
            if (string.IsNullOrWhiteSpace(evaluatorConfigFile))
            {
                Utilities.Diagnostics.Exceptions.Throw(new ArgumentNullException("configFile"), logger);
            }
            if (!File.Exists(evaluatorConfigFile))
            {
                Utilities.Diagnostics.Exceptions.Throw(new FileNotFoundException("cannot find file " + evaluatorConfigFile), logger);
            }

            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
            var classHierarchy = TangFactory.GetTang()
                .GetClassHierarchy(new string[] { typeof(ApplicationIdentifier).Assembly.GetName().Name });
            var evaluatorConfiguration = serializer.FromFile(evaluatorConfigFile, classHierarchy);

            logger.Log(Level.Info, 
                string.Format(CultureInfo.CurrentCulture, "Evaluator Configuration is deserialized from file {0}:", evaluatorConfigFile));

            return evaluatorConfiguration;
        }

        private static void SetCustomTraceListeners(CustomTraceListeners customTraceListener, CustomTraceLevel traceLevel)
        {
            ISet<TraceListener> customTraceListeners = customTraceListener.Listeners;
            foreach (TraceListener listener in customTraceListeners)
            {
                Logger.AddTraceListener(listener);
            }
            logger = Logger.GetLogger(typeof(Evaluator));
            Logger.SetCustomLevel(traceLevel.TraceLevel);
        }

        private static void UnhandledExceptionHandler(object sender, UnhandledExceptionEventArgs e)
        {
            Fail((Exception)e.ExceptionObject);
        }

        // set the handlers for runtimeclock manually
        // we only need runtimestart and runtimestop handlers now
        private static void SetRuntimeHandlers(EvaluatorRuntime evaluatorRuntime, RuntimeClock clock)
        {
            ISet<IObserver<RuntimeStart>> runtimeStarts = new HashSet<IObserver<RuntimeStart>> { evaluatorRuntime };
            clock.InjectedRuntimeStartHandler = new InjectionFutureImpl<ISet<IObserver<RuntimeStart>>>(runtimeStarts);

            ISet<IObserver<RuntimeStop>> runtimeStops = new HashSet<IObserver<RuntimeStop>> { evaluatorRuntime };
            clock.InjectedRuntimeStopHandler = new InjectionFutureImpl<ISet<IObserver<RuntimeStop>>>(runtimeStops);
        }

        private static void Fail(Exception ex)
        {
            var message = "Unhandled exception caught in Evaluator. Current files in the working directory: " +
                          string.Join(", ", Directory.EnumerateFiles(Directory.GetCurrentDirectory(), "*.*", SearchOption.AllDirectories));
            Utilities.Diagnostics.Exceptions.Throw(ex, message, logger);
        }
    }
}
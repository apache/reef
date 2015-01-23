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

using Org.Apache.Reef.Common.Evaluator;
using Org.Apache.Reef.Driver;
using Org.Apache.Reef.Driver.bridge;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Driver.Defaults;
using Org.Apache.Reef.Examples.HelloCLRBridge;
using Org.Apache.Reef.Examples.HelloCLRBridge.Handlers;
using Org.Apache.Reef.IO.Network.Naming;
using Org.Apache.Reef.Tasks;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;
using System;
using System.Collections.Generic;
using System.IO;

namespace Org.Apache.Reef.CLRBridgeClient
{
    public class CLRBridgeClient
    {
        public const string ReefHome = "REEF_HOME";
        public const string DefaultClrFolder = @"lang\java\reef-bridge-project\reef-bridge\dotnetHello";
        public const string DefaultReefJar = @"lang\java\reef-bridge-project\reef-bridge\target\" + Constants.BridgeJarFileName;
        public const string DefaultRunCommand = "run.cmd";

        private static string _clrFolder = null;
        private static string _reefJar = null;
        private static string _className = Constants.BridgeLaunchClass;
        private static string _runCommand = DefaultRunCommand;
        private static string _submitControlForAllocatedEvaluator = "submitContextAndTask"; // submitContext, submitContextAndTask, submitContextAndServiceAndTask

        public static HashSet<string> AppDll
        {
            get
            {
                HashSet<string> appDlls = new HashSet<string>();
                appDlls.Add(typeof(HelloStartHandler).Assembly.GetName().Name);
                appDlls.Add(typeof(HelloTask).Assembly.GetName().Name);
                appDlls.Add(typeof(INameServer).Assembly.GetName().Name);
                return appDlls;
            }
        }

        public static IConfiguration ClrConfigurations
        {
            get
            {
                return DriverBridgeConfiguration.ConfigurationModule
                .Set(DriverBridgeConfiguration.OnDriverStarted, GenericType<HelloStartHandler>.Class)
                .Set(DriverBridgeConfiguration.OnEvaluatorAllocated, GenericType<HelloAllocatedEvaluatorHandler>.Class)
                .Set(DriverBridgeConfiguration.OnEvaluatorAllocated, GenericType<AnotherHelloAllocatedEvaluatorHandler>.Class)
                .Set(DriverBridgeConfiguration.OnContextActive, GenericType<HelloActiveContextHandler>.Class)
                .Set(DriverBridgeConfiguration.OnTaskMessage, GenericType<HelloTaskMessageHandler>.Class)
                .Set(DriverBridgeConfiguration.OnEvaluatorFailed, GenericType<HelloFailedEvaluatorHandler>.Class)
                .Set(DriverBridgeConfiguration.OnTaskFailed, GenericType<HelloFailedTaskHandler>.Class)
                .Set(DriverBridgeConfiguration.OnTaskRunning, GenericType<HelloRunningTaskHandler>.Class)
                .Set(DriverBridgeConfiguration.OnEvaluatorRequested, GenericType<HelloEvaluatorRequestorHandler>.Class)
                .Set(DriverBridgeConfiguration.OnHttpEvent, GenericType<HelloHttpHandler>.Class)
                .Set(DriverBridgeConfiguration.OnEvaluatorCompleted, GenericType<HelloCompletedEvaluatorHandler>.Class)
                .Set(DriverBridgeConfiguration.CustomTraceListeners, GenericType<DefaultCustomTraceListener>.Class)
                .Set(DriverBridgeConfiguration.CustomTraceLevel, Level.Info.ToString())
                .Set(DriverBridgeConfiguration.CommandLineArguments, _submitControlForAllocatedEvaluator) // this is for testing purpose only
                .Set(DriverBridgeConfiguration.OnDriverRestarted, GenericType<HelloRestartHandler>.Class)
                .Set(DriverBridgeConfiguration.OnDriverReconnect, GenericType<DefaultLocalHttpDriverConnection>.Class)
                .Set(DriverBridgeConfiguration.OnDirverRestartContextActive, GenericType<HelloDriverRestartActiveContextHandler>.Class)
                .Set(DriverBridgeConfiguration.OnDriverRestartTaskRunning, GenericType<HelloDriverRestartRunningTaskHandler>.Class)
                .Build();
            }
        }

        public static void Main(string[] args)
        {
            Console.WriteLine("start running client: " + DateTime.Now);
            bool runOnYarn = false;
            if (args != null)
            {
                if (args.Length > 0)
                {
                    runOnYarn = bool.Parse(args[0]);
                }

                if (args.Length > 1)
                {
                    _className = args[1];
                }
                if (args.Length > 2)
                {
                    _clrFolder = args[2];
                }
                if (args.Length > 3)
                {
                    _reefJar = args[3];
                }
                if (args.Length > 4)
                {
                    _runCommand = args[4];
                }
            }

            if (string.IsNullOrWhiteSpace(_reefJar))
            {
                _reefJar = Path.Combine(Environment.GetEnvironmentVariable(ReefHome), DefaultReefJar);
            }

            if (string.IsNullOrWhiteSpace(_clrFolder))
            {
                _clrFolder = Path.Combine(Environment.GetEnvironmentVariable(ReefHome), DefaultClrFolder);
            }

            // Configurable driver submission settings:
            // DriverMemory, default to 512
            // DriverIdentifer, default to "ReefClrBridge" 
            // Submit, default to True. Setting it to false and Java client will construct the driver.config and write to to FS without submitting the job
            // ClientWaitTime, default to -1 which means client will wait till Driver is done
            // SubmissionDirectory, default to a tmp folder on (H)DFS according to the YARN assigned application id, if user sets it, it must be guaranteed to be unique across multiple jobs
            // RunOnYarn, default to false to run on local runtime.
            // UpdateJarBeforeSubmission, default to true. Setting it to false can reduce startup time, but only if the jar file already contains all application dlls.
            // JavaLogLevel, default to INFO. 
            DriverSubmissionSettings driverSubmissionSettings = new DriverSubmissionSettings()
                                                                    {
                                                                        RunOnYarn = runOnYarn,
                                                                        SubmissionDirectory = "reefClrBridgeTmp/job_" + Guid.NewGuid().ToString("N").Substring(0, 8)
                                                                    };

            Console.WriteLine("start calling Run: " + DateTime.Now);
            ClrClientHelper.Run(AppDll, ClrConfigurations, driverSubmissionSettings, _reefJar, _runCommand, _clrFolder, _className);
        }
    }
}

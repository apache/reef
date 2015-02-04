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

using System.Collections.Generic;

namespace Org.Apache.Reef.Driver
{
    public class Constants
    {
        public const ulong NullHandler = 0;

        public const string ClassHierarachyBin = "clrClassHierarchy.bin";

        public const string GlobalUserSuppliedJavaLibraries = "userSuppliedGlobalLibraries.txt";

        public const int DefaultMemoryGranularity = 1024;

        public const int HandlersNumber = 17;

        public const string EvaluatorRequestorHandler = "EvaluatorRequestor";

        public const string AllocatedEvaluatorHandler = "AllocatedEvaluator";

        public const string CompletedEvaluatorHandler = "CompletedEvaluator";

        public const string ActiveContextHandler = "ActiveContext";

        public const string ClosedContextHandler = "ClosedContext";

        public const string FailedContextHandler = "FailedContext";

        public const string ContextMessageHandler = "ContextMessage";

        public const string TaskMessageHandler = "TaskMessage";

        public const string FailedTaskHandler = "FailedTask";

        public const string RunningTaskHandler = "RunningTask";

        public const string FailedEvaluatorHandler = "FailedEvaluator";

        public const string CompletedTaskHandler = "CompletedTask";

        public const string SuspendedTaskHandler = "SuspendedTask";

        public const string HttpServerHandler = "HttpServerHandler";

        public const string DriverRestartHandler = "DriverRestart";

        public const string DriverRestartActiveContextHandler = "DriverRestartActiveContext";

        public const string DriverRestartRunningTaskHandler = "DriverRestartRunningTask";

        public const string DriverBridgeConfiguration = Common.Constants.ClrBridgeRuntimeConfiguration;

        public const string DriverAppDirectory = "ReefDriverAppDlls";

        public const string BridgeJarFileName = "reef-bridge-0.11.0-incubating-SNAPSHOT-shaded.jar";

        public const string BridgeLaunchClass = "org.apache.reef.javabridge.generic.Launch";

        public const string BridgeLaunchHeadlessClass = "org.apache.reef.javabridge.generic.LaunchHeadless";

        public const string DirectLauncherClass = "org.apache.reef.runtime.common.Launcher";

        public const string JavaToCLRLoggingConfig = "-Djava.util.logging.config.class=org.apache.reef.util.logging.CLRLoggingConfig";

        public const string JavaVerboseLoggingConfig = "-Djava.util.logging.config.class=org.apache.reef.util.logging.Config";

        public static Dictionary<string, int> Handlers
        {
            get
            {
                return
                    new Dictionary<string, int>()
                    {
                        { EvaluatorRequestorHandler, 0 },
                        { AllocatedEvaluatorHandler, 1 },
                        { ActiveContextHandler, 2 },
                        { TaskMessageHandler, 3 },
                        { FailedTaskHandler, 4 },
                        { FailedEvaluatorHandler, 5 },
                        { HttpServerHandler, 6 },
                        { CompletedTaskHandler, 7 },
                        { RunningTaskHandler, 8 },
                        { SuspendedTaskHandler, 9 },
                        { CompletedEvaluatorHandler, 10 },
                        { ClosedContextHandler, 11 },
                        { FailedContextHandler, 12 },
                        { ContextMessageHandler, 13 },
                        { DriverRestartHandler, 14 },
                        { DriverRestartActiveContextHandler, 15 },
                        { DriverRestartRunningTaskHandler, 16 },
                    };
            }
        }
    }
}

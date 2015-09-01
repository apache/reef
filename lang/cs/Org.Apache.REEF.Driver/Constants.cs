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

namespace Org.Apache.REEF.Driver
{
    public class Constants
    {
        /// <summary>
        /// Null handler that is not used on Java side.
        /// </summary>
        public const ulong NullHandler = 0;

        /// <summary>
        /// The class hierarchy file from .NET.
        /// </summary>
        public const string ClassHierarachyBin = "clrClassHierarchy.bin";

        /// <summary>
        /// The file containing user supplied libaries.
        /// </summary>
        public const string GlobalUserSuppliedJavaLibraries = "userSuppliedGlobalLibraries.txt";

        /// <summary>
        /// The default memory granularity for evaluators.
        /// </summary>
        public const int DefaultMemoryGranularity = 1024;

        /// <summary>
        /// The number of handlers total. Tightly coupled with Java.
        /// </summary>
        public const int HandlersNumber = 18;

        /// <summary>
        /// The name for EvaluatorRequestorHandler. Tightly coupled with Java.
        /// </summary>
        public const string EvaluatorRequestorHandler = "EvaluatorRequestor";

        /// <summary>
        /// The name for AllocatedEvaluatorHandler. Tightly coupled with Java.
        /// </summary>
        public const string AllocatedEvaluatorHandler = "AllocatedEvaluator";

        /// <summary>
        /// The name for CompletedEvaluatorHandler. Tightly coupled with Java.
        /// </summary>
        public const string CompletedEvaluatorHandler = "CompletedEvaluator";

        /// <summary>
        /// The name for ActiveContextHandler. Tightly coupled with Java.
        /// </summary>
        public const string ActiveContextHandler = "ActiveContext";

        /// <summary>
        /// The name for ClosedContextHandler. Tightly coupled with Java.
        /// </summary>
        public const string ClosedContextHandler = "ClosedContext";

        /// <summary>
        /// The name for FailedContextHandler. Tightly coupled with Java.
        /// </summary>
        public const string FailedContextHandler = "FailedContext";
        
        /// <summary>
        /// The name for ContextMessageHandler. Tightly coupled with Java.
        /// </summary>
        public const string ContextMessageHandler = "ContextMessage";

        /// <summary>
        /// The name for TaskMessageHandler. Tightly coupled with Java.
        /// </summary>
        public const string TaskMessageHandler = "TaskMessage";

        /// <summary>
        /// The name for FailedTaskHandler. Tightly coupled with Java.
        /// </summary>
        public const string FailedTaskHandler = "FailedTask";

        /// <summary>
        /// The name for RunningTaskHandler. Tightly coupled with Java.
        /// </summary>
        public const string RunningTaskHandler = "RunningTask";

        /// <summary>
        /// The name for FailedEvaluatorHandler. Tightly coupled with Java.
        /// </summary>
        public const string FailedEvaluatorHandler = "FailedEvaluator";

        /// <summary>
        /// The name for CompletedTaskHandler. Tightly coupled with Java.
        /// </summary>
        public const string CompletedTaskHandler = "CompletedTask";

        /// <summary>
        /// The name for SuspendedTaskHandler. Tightly coupled with Java.
        /// </summary>
        public const string SuspendedTaskHandler = "SuspendedTask";

        /// <summary>
        /// The name for HttpServerHandler. Tightly coupled with Java.
        /// </summary>
        public const string HttpServerHandler = "HttpServerHandler";

        /// <summary>
        /// The name for DriverRestartActiveContextHandler. Tightly coupled with Java.
        /// </summary>
        public const string DriverRestartActiveContextHandler = "DriverRestartActiveContext";

        /// <summary>
        /// The name for DriverRestartRunningTaskHandler. Tightly coupled with Java.
        /// </summary>
        public const string DriverRestartRunningTaskHandler = "DriverRestartRunningTask";

        /// <summary>
        /// The name for DriverRestartCompletedHandler. Tightly coupled with Java.
        /// </summary>
        public const string DriverRestartCompletedHandler = "DriverRestartCompleted";

        /// <summary>
        /// The name for DriverRestartFailedEvaluatorHandler. Tightly coupled with Java
        /// </summary>
        public const string DriverRestartFailedEvaluatorHandler = "DriverRestartFailedEvaluator";

        [Obsolete(message:"Use REEFFileNames instead.")]
        public const string DriverBridgeConfiguration = Common.Constants.ClrBridgeRuntimeConfiguration;

        /// <summary>
        /// The directory to load driver DLLs.
        /// </summary>
        public const string DriverAppDirectory = "ReefDriverAppDlls";
        
        /// <summary>
        /// The bridge JAR name.
        /// </summary>
        public const string JavaBridgeJarFileName = "reef-bridge-java-0.13.0-incubating-SNAPSHOT-shaded.jar";

        public const string BridgeLaunchClass = "org.apache.reef.javabridge.generic.Launch";

        [Obsolete(message: "Deprecated in 0.13. Use BridgeLaunchClass instead.")]
        public const string BridgeLaunchHeadlessClass = "org.apache.reef.javabridge.generic.LaunchHeadless";

        /// <summary>
        /// The direct launcher class, deprecated in 0.13, please use DirectREEFLauncherClass instead.
        /// </summary>
        [Obsolete("Deprecated in 0.13, please use DirectREEFLauncherClass instead.")]
        public const string DirectLauncherClass = "org.apache.reef.runtime.common.Launcher";

        /// <summary>
        /// The direct launcher class.
        /// </summary>
        public const string DirectREEFLauncherClass = "org.apache.reef.runtime.common.REEFLauncher";

        /// <summary>
        /// Configuration for Java CLR logging.
        /// </summary>
        public const string JavaToCLRLoggingConfig = "-Djava.util.logging.config.class=org.apache.reef.util.logging.CLRLoggingConfig";

        /// <summary>
        /// Configuration for Java verbose logging.
        /// </summary>
        public const string JavaVerboseLoggingConfig = "-Djava.util.logging.config.class=org.apache.reef.util.logging.Config";

        /// <summary>
        /// A dictionary of handler constants to handler descriptors.
        /// </summary>
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
                        { DriverRestartActiveContextHandler, 14 },
                        { DriverRestartRunningTaskHandler, 15 },
                        { DriverRestartCompletedHandler, 16 },
                        { DriverRestartFailedEvaluatorHandler, 17 }
                    };
            }
        }
    }
}

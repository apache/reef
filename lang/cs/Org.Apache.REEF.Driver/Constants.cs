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

namespace Org.Apache.REEF.Driver
{
    // TODO[REEF-842] Act on the obsoletes
    public class Constants
    {
        /// <summary>
        /// Null handler that is not used on Java side.
        /// </summary>
        public const ulong NullHandler = 0;

        /// <summary>
        /// The class hierarchy file from .NET.
        /// </summary>
        [Obsolete("Deprecated in 0.14, please use ClassHierarchyBin instead.")]
        public const string ClassHierarachyBin = "clrClassHierarchy.bin";

        /// <summary>
        /// The class hierarchy file from .NET.
        /// </summary>
        public const string ClassHierarchyBin = "clrClassHierarchy.bin";

        /// <summary>
        /// The file containing user supplied libraries.
        /// </summary>
        public const string GlobalUserSuppliedJavaLibraries = "userSuppliedGlobalLibraries.txt";

        /// <summary>
        /// The default memory granularity for evaluators.
        /// </summary>
        public const int DefaultMemoryGranularity = 1024;

        [Obsolete(message: "Use REEFFileNames instead.")]
        public const string DriverBridgeConfiguration = Common.Constants.ClrBridgeRuntimeConfiguration;

        /// <summary>
        /// The directory to load driver DLLs.
        /// </summary>
        public const string DriverAppDirectory = "ReefDriverAppDlls";
        
        /// <summary>
        /// The bridge JAR name.
        /// </summary>
        public const string JavaBridgeJarFileName = "reef-bridge-java-0.14.0-SNAPSHOT-shaded.jar";

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
    }
}

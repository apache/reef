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

using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using System;
using System.IO;

namespace Org.Apache.REEF.Bridge.Core.Common.Client.Config
{
    /// <summary>
    /// Configuration Module for the driver runtime, which corresponds to the
    /// Java driver.
    /// </summary>
    public sealed class DriverRuntimeConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The Job identifier.
        /// </summary>
        public static readonly RequiredParameter<string> JobId = new RequiredParameter<string>();

        /// <summary>
        /// The Job submission directory where related job files are staged for submission.
        /// </summary>
        public static readonly OptionalParameter<string> JobSubmissionDirectory = new OptionalParameter<string>();

        /// <summary>
        /// How many CPU cores should be dedicated to the driver.
        /// </summary>
        public static readonly OptionalParameter<int> DriverCpuCores = new OptionalParameter<int>();

        /// <summary>
        /// How much memory in MegaBytes should be dedicated to the driver.
        /// </summary>
        public static readonly OptionalParameter<int> DriverMemory = new OptionalParameter<int>();

        /// <summary>
        /// Assembiles that should be present on the driver and all evaluators.
        /// </summary>
        public static readonly OptionalParameter<string> GlobalAssemblies = new OptionalParameter<string>();

        /// <summary>
        /// Assembiles that should be present only on the driver.
        /// </summary>
        public static readonly OptionalParameter<string> LocalAssemblies = new OptionalParameter<string>();

        /// <summary>
        /// Files that should be present on the driver and all evaluators.
        /// </summary>
        public static readonly OptionalParameter<string> GlobalFiles = new OptionalParameter<string>();

        /// <summary>
        /// Files that should be present only on the driver.
        /// </summary>
        public static readonly OptionalParameter<string> LocalFiles = new OptionalParameter<string>();

        /// <summary>
        /// Indicates whether support for driver restart should be enabled.
        /// </summary>
        public static readonly OptionalParameter<bool> EnableDriverRestart = new OptionalParameter<bool>();

        /// <summary>
        /// How long should we wait for evaluators to checkin with the restarted driver before
        /// considering them dead.
        /// </summary>
        public static readonly OptionalParameter<int> RestartEvaluatorRecoverySeconds = new OptionalParameter<int>();

        /// <summary>
        /// The start TCP port to use for driver evaluator communication.
        /// </summary>
        public static readonly OptionalParameter<int> TcpPortRangeBegin = new OptionalParameter<int>();

        /// <summary>
        /// The port range for driver evaluator communication.
        /// </summary>
        public static readonly OptionalParameter<int> TcpPortRangeCount = new OptionalParameter<int>();

        /// <summary>
        /// How many ports should we try before giving up.
        /// </summary>
        public static readonly OptionalParameter<int> TcpPortRangeTryCount = new OptionalParameter<int>();

        /// <summary>
        /// The intended operating system on which the driver and evaluators will run.
        /// </summary>
        public static readonly OptionalImpl<OsType> OsType = new OptionalImpl<OsType>();

        public static ConfigurationModule ConfigurationModule => new DriverRuntimeConfiguration()
            .BindImplementation(GenericType<OsType>.Class, OsType)
            .BindNamedParameter(GenericType<DriverRuntimeParameters.JobId>.Class, JobId)
            .BindNamedParameter(GenericType<DriverRuntimeParameters.JobSubmissionDirectory>.Class, JobSubmissionDirectory)
            .BindNamedParameter(GenericType<DriverRuntimeParameters.DriverCpuCores>.Class, DriverCpuCores)
            .BindNamedParameter(GenericType<DriverRuntimeParameters.DriverMemory>.Class, DriverMemory)
            .BindNamedParameter(GenericType<DriverRuntimeParameters.EnableDriverRestart>.Class, EnableDriverRestart)
            .BindNamedParameter(GenericType<DriverRuntimeParameters.RestartEvaluatorRecoverySeconds>.Class, RestartEvaluatorRecoverySeconds)
            .BindNamedParameter(GenericType<DriverRuntimeParameters.TcpPortRangeBegin>.Class, TcpPortRangeBegin)
            .BindNamedParameter(GenericType<DriverRuntimeParameters.TcpPortRangeCount>.Class, TcpPortRangeCount)
            .BindNamedParameter(GenericType<DriverRuntimeParameters.TcpPortRangeTryCount>.Class, TcpPortRangeTryCount)
            .BindSetEntry(GenericType<DriverRuntimeParameters.GlobalAssemblies>.Class, GlobalAssemblies)
            .BindSetEntry(GenericType<DriverRuntimeParameters.LocalAssemblies>.Class, LocalAssemblies)
            .BindSetEntry(GenericType<DriverRuntimeParameters.GlobalFiles>.Class, GlobalFiles)
            .BindSetEntry(GenericType<DriverRuntimeParameters.LocalFiles>.Class, LocalFiles)
            .Build();

        /// <summary>
        /// Add the assembly needed for the given Type to the driver.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static ConfigurationModule AddLocalAssemblyForType(ConfigurationModule conf, Type type)
        {
            return conf.Set(LocalAssemblies, GetAssemblyPathForType(type));
        }

        /// <summary>
        /// Add the assembly needed for the given Type to all containers.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static ConfigurationModule AddGlobalAssemblyForType(ConfigurationModule conf, Type type)
        {
            return conf.Set(GlobalAssemblies, GetAssemblyPathForType(type));
        }

        /// <summary>
        /// Finds the path to the assembly the given Type was loaded from.
        /// </summary>
        private static string GetAssemblyPathForType(Type type)
        {
            var path = Uri.UnescapeDataString(new UriBuilder(type.Assembly.CodeBase).Path);
            return Path.GetFullPath(path);
        }
    }
}
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

using Org.Apache.REEF.Bridge.Core.Common.Client.Config;
using Org.Apache.REEF.Bridge.Core.Proto;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using System.Collections.Generic;

namespace Org.Apache.REEF.Bridge.Core.Grpc.Client
{
    internal sealed class DriverClientParameters
    {
        public DriverClientConfiguration Proto { get; }

        [Inject]
        private DriverClientParameters(
            OsType osType,
            [Parameter(Value = typeof(DriverRuntimeParameters.JobId))] string jobId,
            [Parameter(Value = typeof(DriverRuntimeParameters.JobSubmissionDirectory))] string jobSubmissionDirectory,
            [Parameter(Value = typeof(DriverRuntimeParameters.DriverCpuCores))] int driverCpuCores,
            [Parameter(Value = typeof(DriverRuntimeParameters.DriverMemory))] int driverMemory,
            [Parameter(Value = typeof(DriverRuntimeParameters.GlobalAssemblies))] ISet<string> globalAssemblies,
            [Parameter(Value = typeof(DriverRuntimeParameters.LocalAssemblies))] ISet<string> localAssemblies,
            [Parameter(Value = typeof(DriverRuntimeParameters.GlobalFiles))] ISet<string> globalFiles,
            [Parameter(Value = typeof(DriverRuntimeParameters.LocalFiles))] ISet<string> localFiles,
            [Parameter(Value = typeof(DriverRuntimeParameters.EnableDriverRestart))] bool enableDriverRestart,
            [Parameter(Value = typeof(DriverRuntimeParameters.TcpPortRangeBegin))] int tcpPortRangeBegin,
            [Parameter(Value = typeof(DriverRuntimeParameters.TcpPortRangeCount))] int tcpPortRangeCount,
            [Parameter(Value = typeof(DriverRuntimeParameters.TcpPortRangeTryCount))] int tcpPortRangeTryCount,
            [Parameter(Value = typeof(DriverRuntimeParameters.RestartEvaluatorRecoverySeconds))] int restartEvaluatorRecoverySeconds)
        {
            Proto = new DriverClientConfiguration()
            {
                Jobid = jobId,
                DriverJobSubmissionDirectory = jobSubmissionDirectory,
                CpuCores = (uint)driverCpuCores,
                MemoryMb = (uint)driverMemory,
                DriverRestartEnable = enableDriverRestart,
                DriverRestartEvaluatorRecoverySeconds = (uint)restartEvaluatorRecoverySeconds,
                TcpPortRangeBegin = (uint)tcpPortRangeBegin,
                TcpPortRangeCount = (uint)tcpPortRangeCount,
                TcpPortRangeTryCount = (uint)tcpPortRangeTryCount
            };
            Proto.GlobalLibraries.Add(globalAssemblies);
            Proto.GlobalFiles.Add(globalFiles);
            Proto.LocalLibraries.Add(localAssemblies);
            Proto.LocalFiles.Add(localFiles);
            switch (osType.Type)
            {
                case OsType.Os.Windows:
                    Proto.OperatingSystem = DriverClientConfiguration.Types.OS.Windows;
                    break;

                case OsType.Os.Linux:
                    Proto.OperatingSystem = DriverClientConfiguration.Types.OS.Linux;
                    break;

                default:
                    throw new IllegalStateException("Unknown operating system type " + osType.Type);
            }
        }
    }
}
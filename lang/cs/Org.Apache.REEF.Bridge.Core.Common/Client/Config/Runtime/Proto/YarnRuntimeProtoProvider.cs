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

using Org.Apache.REEF.Bridge.Core.Proto;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using System.Collections.Generic;

namespace Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime.Proto
{
    internal sealed class YarnRuntimeProtoProvider : IRuntimeProtoProvider
    {
        private readonly Core.Proto.YarnRuntimeParameters _yarnRuntimeParameters;

        [Inject]
        private YarnRuntimeProtoProvider(
            [Parameter(Value = typeof(YarnRuntimeParameters.JobPriority))] int jobPriority,
            [Parameter(Value = typeof(YarnRuntimeParameters.JobQueue))] string jobQueue,
            [Parameter(Value = typeof(YarnRuntimeParameters.JobSubmissionDirectoryPrefix))] string jobSubmissionDirectoryPrefix,
            [Parameter(Value = typeof(YarnRuntimeParameters.SecurityTokenStrings))] ISet<string> securityTokenStrings,
            [Parameter(Value = typeof(YarnRuntimeParameters.UnmanagedDriver))] bool unmanagedDriver,
            [Parameter(Value = typeof(YarnRuntimeParameters.FileSystemUrl))] string fileSystemUrl)
        {
            if (jobPriority < 0)
            {
                throw new IllegalStateException("Job Priority must be greater than or equal to zero");
            }
            _yarnRuntimeParameters = new Core.Proto.YarnRuntimeParameters()
            {
                Priority = (uint)jobPriority,
                Queue = jobQueue,
                JobSubmissionDirectoryPrefix = jobSubmissionDirectoryPrefix,
                UnmangedDriver = unmanagedDriver,
                FilesystemUrl = fileSystemUrl
            };
        }

        public void SetParameters(DriverClientConfiguration driverClientConfiguration)
        {
            driverClientConfiguration.YarnRuntime = _yarnRuntimeParameters;
        }
    }
}
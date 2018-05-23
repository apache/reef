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
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Bridge.Core.Common.Client.Config
{
    public sealed class DriverRuntimeParameters
    {

        [NamedParameter("the job identifier")]
        public sealed class JobId : Name<string>
        {
        }

        [NamedParameter("The job submission directory", defaultValue: "")]
        public sealed class JobSubmissionDirectory : Name<string>
        {
        }

        [NamedParameter("Number of cpu cores", defaultValue: "1")]
        public sealed class DriverCpuCores : Name<int>
        {
        }

        [NamedParameter("The amount of driver memory in MB", defaultValue: "512")]
        public sealed class DriverMemory : Name<int>
        {
        }

        [NamedParameter("enable driver restart", defaultValue: "true")]
        public sealed class EnableDriverRestart : Name<bool>
        {
        }

        [NamedParameter("the amount of time to wait for evaluators to recover after failed driver", defaultValue: "0")]
        public sealed class RestartEvaluatorRecoverySeconds : Name<int>
        {
        }

        [NamedParameter("Driver and Evaluator assemblies.")]
        public sealed class GlobalAssemblies : Name<ISet<string>>
        {
        }

        [NamedParameter("Driver only related assemblies.")]
        public sealed class LocalAssemblies : Name<ISet<string>>
        {
        }

        [NamedParameter("Driver and Evaluator file resources.")]
        public sealed class GlobalFiles : Name<ISet<string>>
        {
        }

        [NamedParameter("Driver only related file resources.")]
        public sealed class LocalFiles : Name<ISet<string>>
        {
        }

        [NamedParameter("tcp port range begin", defaultValue: "0")]
        public sealed class TcpPortRangeBegin : Name<int>
        {
        }

        [NamedParameter("tcp port range count", defaultValue: "0")]
        public sealed class TcpPortRangeCount : Name<int>
        {
        }

        [NamedParameter("tcp port range try count", defaultValue: "0")]
        public sealed class TcpPortRangeTryCount : Name<int>
        {
        }
    }
}

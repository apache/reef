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

namespace Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime.Proto
{
    internal sealed class LocalRuntimeProtoProvider : IRuntimeProtoProvider
    {
        private readonly Core.Proto.LocalRuntimeParameters _localRuntimeParameters;

        [Inject]
        private LocalRuntimeProtoProvider(
            [Parameter(Value = typeof(LocalRuntimeParameters.LocalRuntimeDirectory))] string localRuntimeDirectory,
            [Parameter(Value = typeof(LocalRuntimeParameters.NumberOfEvaluators))] int numberOfEvaluators)
        {
            if (numberOfEvaluators <= 0)
            {
                throw new IllegalStateException("Number of evaluators must be greater than zero");
            }
            _localRuntimeParameters = new Core.Proto.LocalRuntimeParameters()
            {
                MaxNumberOfEvaluators = (uint)numberOfEvaluators,
                RuntimeRootFolder = localRuntimeDirectory
            };
        }

        public void SetParameters(DriverClientConfiguration driverClientConfiguration)
        {
            driverClientConfiguration.LocalRuntime = _localRuntimeParameters;
        }
    }
}
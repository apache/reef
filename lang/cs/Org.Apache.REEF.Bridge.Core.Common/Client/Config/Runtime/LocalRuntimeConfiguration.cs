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

using Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime.Proto;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime
{
    public sealed class LocalRuntimeConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The number of threads or evaluators available to the resourcemanager.
        /// </summary>
        /// <remarks>
        /// This is the upper limit on the number of
        /// Evaluators that the local resourcemanager will hand out concurrently. This simulates the size of a physical cluster
        /// in terms of the number of slots available on it with one important caveat: The Driver is not counted against this
        /// number.
        /// </remarks>
        public static readonly OptionalParameter<int> NumberOfEvaluators = new OptionalParameter<int>();

        /// <summary>
        /// The folder in which the sub-folders, one per job, will be created.
        /// </summary>
        /// <remarks>
        /// If none is given, the temp directory is used.
        /// </remarks>
        public static readonly OptionalParameter<string> RuntimeFolder = new OptionalParameter<string>();

        public static ConfigurationModule ConfigurationModule = new LocalRuntimeConfiguration()
            .BindImplementation(GenericType<IRuntimeProtoProvider>.Class, GenericType<LocalRuntimeProtoProvider>.Class)
            .BindNamedParameter(GenericType<LocalRuntimeParameters.LocalRuntimeDirectory>.Class, RuntimeFolder)
            .BindNamedParameter(GenericType<LocalRuntimeParameters.NumberOfEvaluators>.Class, NumberOfEvaluators)
            .Build();
    }
}
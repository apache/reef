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
    public sealed class YarnRuntimeConfiguration : ConfigurationModuleBuilder
    {
        public static readonly OptionalParameter<int> JobPriority = new OptionalParameter<int>();
        public static readonly OptionalParameter<string> JobQueue = new OptionalParameter<string>();
        public static readonly OptionalParameter<string> JobSubmissionDirectoryPrefix = new OptionalParameter<string>();
        public static readonly OptionalParameter<string> SecurityTokenStrings = new OptionalParameter<string>();
        public static readonly OptionalParameter<bool> UnmanagedDriver = new OptionalParameter<bool>();

        /// <summary>
        /// URL for store. For Hadoop file system, it is set in fs.defaultFS as default by YARN environment. Client doesn't need to
        /// specify it. For Data Lake, Yarn applications are required to set the complete path by themselves
        /// e.g. adl://reefadl.azuredatalakestore.net
        /// </summary>
        public static readonly OptionalParameter<string> FileSystemUrl = new OptionalParameter<string>();

        public static ConfigurationModule ConfigurationModule = new YarnRuntimeConfiguration()
            .BindImplementation(GenericType<IRuntimeProtoProvider>.Class, GenericType<YarnRuntimeProtoProvider>.Class)
            .BindNamedParameter(GenericType<YarnRuntimeParameters.JobPriority>.Class, JobPriority)
            .BindNamedParameter(GenericType<YarnRuntimeParameters.JobSubmissionDirectoryPrefix>.Class, JobSubmissionDirectoryPrefix)
            .BindNamedParameter(GenericType<YarnRuntimeParameters.JobQueue>.Class, JobQueue)
            .BindSetEntry(GenericType<YarnRuntimeParameters.SecurityTokenStrings>.Class, SecurityTokenStrings)
            .BindNamedParameter(GenericType<YarnRuntimeParameters.FileSystemUrl>.Class, FileSystemUrl)
            .BindNamedParameter(GenericType<YarnRuntimeParameters.UnmanagedDriver>.Class, UnmanagedDriver)
            .Build();
    }
}
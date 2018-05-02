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

using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.IMRU.OnREEF.CheckpointHandler
{
    /// <summary>
    /// Configuration module builder for checkpoint handler
    /// </summary>
    public sealed class CheckpointConfigurationBuilder : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The check point sttate file path. In cluster, should be a path in remote storage.
        /// </summary>
        public static readonly RequiredParameter<string> CheckpointFilePath = new RequiredParameter<string>();

        /// <summary>
        /// Codec that is to encode and decode state object.
        /// </summary>
        public static readonly RequiredImpl<ICodec<ITaskState>> TaskStateCodec = new RequiredImpl<ICodec<ITaskState>>();

        /// <summary>
        /// Configuration module for checkpoint handler.
        /// </summary>
        public static readonly ConfigurationModule ConfigurationModule = new CheckpointConfigurationBuilder()
            .BindNamedParameter(GenericType<CheckpointFilePath>.Class, CheckpointFilePath)
            .BindImplementation(GenericType<IIMRUCheckpointHandler>.Class, GenericType<IMRUCheckpointHandler>.Class)
            .BindImplementation(GenericType<ICodec<ITaskState>>.Class, TaskStateCodec)
            .Build();
    }
}
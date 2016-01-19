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

using Org.Apache.REEF.IO.PartitionedData.FileSystem.Parameters;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.PartitionedData.FileSystem
{
    /// <summary>
    /// This configuration module set FileSystemDataSet as IPartitionedDataSet.
    /// It also set required parameters for injecting FileSystemDataSet
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class FileSystemInputPartitionConfiguration<T> : ConfigurationModuleBuilder
    {
        /// <summary>
        /// This parameter contains file paths required for file partition dataset
        /// </summary>
        public static readonly RequiredParameter<string> FilePathForPartitions = new RequiredParameter<string>();

        /// <summary>
        /// This is serialized configuration for ISerializer
        /// </summary>
        public static readonly RequiredParameter<string> FileSerializerConfig = new RequiredParameter<string>();

        /// <summary>
        /// This configuration module set FileSystemDataSet as IPartitionedDataSet.
        /// It also set required parameters for injecting FileSystemDataSet
        /// </summary>
        public static ConfigurationModule ConfigurationModule = new FileSystemInputPartitionConfiguration<T>()
            .BindImplementation(GenericType<IPartitionedInputDataSet>.Class, GenericType<FileSystemPartitionInputDataSet<T>>.Class)
            .BindSetEntry(GenericType<FilePathsForInputPartitions>.Class, FilePathForPartitions)
            .BindNamedParameter(GenericType<FileDeSerializerConfigString>.Class, FileSerializerConfig)
            .Build();
    }
}

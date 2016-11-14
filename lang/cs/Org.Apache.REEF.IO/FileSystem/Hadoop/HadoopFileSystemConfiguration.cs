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

using Org.Apache.REEF.Common.Client.Parameters;
using Org.Apache.REEF.IO.FileSystem.Hadoop.Parameters;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.FileSystem.Hadoop
{
    /// <summary>
    /// Configuration Module for the (command based) Hadoop file system implementation of IFileSystem.
    /// </summary>
    /// <remarks>
    /// This IFileSystem implementation is enormously slow as it spawns a new JVM per API call. Avoid if you have better means
    /// of file system access.
    /// Also, Stream-based operations are not supported.
    /// </remarks>
    public sealed class HadoopFileSystemConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The number of times each HDFS command will be retried. Defaults to 3.
        /// </summary>
        public static readonly OptionalParameter<int> CommandRetries = new OptionalParameter<int>();

        /// <summary>
        /// The timeout (in milliseconds) for HDFS commands. Defaults to 300000 (5 minutes).
        /// </summary>
        public static readonly OptionalParameter<int> CommandTimeOut = new OptionalParameter<int>();

        /// <summary>
        /// The folder in which Hadoop is installed. Defaults to %HADOOP_HOME%.
        /// </summary>
        public static readonly OptionalParameter<string> HadoopHome = new OptionalParameter<string>();

        /// <summary>
        /// Set HadoopFileSystemConfigurationProvider to DriverConfigurationProviders.
        /// Set all the parameters needed for injecting HadoopFileSystemConfigurationProvider.
        /// </summary>
        public static readonly ConfigurationModule ConfigurationModule = new HadoopFileSystemConfiguration()
            .BindSetEntry<DriverConfigurationProviders, HadoopFileSystemConfigurationProvider, IConfigurationProvider>(
                GenericType<DriverConfigurationProviders>.Class, GenericType<HadoopFileSystemConfigurationProvider>.Class)
            .BindImplementation(GenericType<IFileSystem>.Class, GenericType<HadoopFileSystem>.Class)
            .BindNamedParameter(GenericType<NumberOfRetries>.Class, CommandRetries)
            .BindNamedParameter(GenericType<CommandTimeOut>.Class, CommandTimeOut)
            .BindNamedParameter(GenericType<HadoopHome>.Class, HadoopHome)
            .Build();
    }
}
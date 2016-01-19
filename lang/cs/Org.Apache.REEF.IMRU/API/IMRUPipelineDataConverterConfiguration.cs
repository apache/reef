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

using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IMRU.API
{
    /// <summary>
    /// A configuration module for The PipelineDataConverter used for chunking 
    /// and dechunking TMapInput and TMapOutput
    /// </summary>
    /// <typeparam name="T">Generic type</typeparam>
    public sealed class IMRUPipelineDataConverterConfiguration<T> : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The PipelineDataConverter used for chunking and
        ///  dechunking TMapInput and TMapOutput
        /// </summary>
        public static readonly
            RequiredImpl<IPipelineDataConverter<T>> MapInputPiplelineDataConverter =
                new RequiredImpl<IPipelineDataConverter<T>>();

        /// <summary>
        /// Configuration module
        /// </summary>
        public static ConfigurationModule ConfigurationModule =
            new IMRUPipelineDataConverterConfiguration<T>()
                .BindImplementation(GenericType<IPipelineDataConverter<T>>.Class, MapInputPiplelineDataConverter)
                .Build();
    }
}
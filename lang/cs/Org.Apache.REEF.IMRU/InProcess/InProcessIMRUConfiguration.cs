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
using Org.Apache.REEF.IMRU.InProcess.Parameters;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IMRU.InProcess
{
    /// <summary>
    /// Configuration module for the in-process IMRU.
    /// </summary>
    public sealed class InProcessIMRUConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The number of Mappers to instantiate.
        /// </summary>
        public static readonly OptionalParameter<int> NumberOfMappers = new OptionalParameter<int>();

        /// <summary>
        /// Configuration module
        /// </summary>
        public static ConfigurationModule ConfigurationModule =
            new InProcessIMRUConfiguration()
                .BindImplementation(GenericType<IIMRUClient>.Class,
                    GenericType<InProcessIMRUClient>.Class)
                .BindNamedParameter(GenericType<NumberOfMappers>.Class, NumberOfMappers)
                .Build();
    }
}
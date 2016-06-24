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

using System.Globalization;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.IMRU.Examples.NaturalSum
{
    /// <summary>
    /// Configuration generator for assiging unique ids to each mapper.
    /// </summary>
    internal sealed class NaturalSumPerMapperConfigGenerator : IPerMapperConfigGenerator
    {
        [Inject]
        private NaturalSumPerMapperConfigGenerator()
        {
        }

        /// <summary>
        /// Assign unique mapper id to each mapper. Ids start from 1.
        /// </summary>
        /// <param name="currentPartitionNumber">partition index of current mapper</param>
        /// <param name="totalMappers">number of mappers, not used</param>
        /// <returns>Configuration for <code>currentPartitionNumber</code>-th mapper</returns>
        public IConfiguration GetMapperConfiguration(int currentPartitionNumber, int totalMappers)
        {
            var mapperId = currentPartitionNumber + 1;
            return TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter(typeof(MapperId), mapperId.ToString(CultureInfo.InvariantCulture))
                .Build();
        }

        [NamedParameter("Id number for each mapper")]
        internal sealed class MapperId : Name<int>
        {
        }
    }
}

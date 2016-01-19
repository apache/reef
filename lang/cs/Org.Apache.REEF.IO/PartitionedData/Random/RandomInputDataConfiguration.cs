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

using Org.Apache.REEF.IO.PartitionedData.Random.Parameters;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.PartitionedData.Random
{
    /// <summary>
    /// Configuration module for the random dataset generator.
    /// </summary>
    public sealed class RandomInputDataConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The number of doubles to serialize into the stream per partition.
        /// </summary>
        public static readonly OptionalParameter<int> NumberOfDoublesPerPartition = new OptionalParameter<int>();

        /// <summary>
        /// Number of random partitions to create.
        /// </summary>
        public static readonly OptionalParameter<int> NumberOfPartitions = new OptionalParameter<int>();

        public static ConfigurationModule ConfigurationModule = new RandomInputDataConfiguration()
            .BindImplementation(GenericType<IPartitionedInputDataSet>.Class, GenericType<RandomInputDataSet>.Class)
            .BindNamedParameter(GenericType<NumberOfPartitions>.Class, NumberOfPartitions)
            .BindNamedParameter(GenericType<NumberOfDoublesPerPartition>.Class, NumberOfDoublesPerPartition)
            .Build();
    }
}
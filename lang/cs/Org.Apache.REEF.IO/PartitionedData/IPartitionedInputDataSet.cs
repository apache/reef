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

using System.Collections.Generic;

namespace Org.Apache.REEF.IO.PartitionedData
{
    /// <summary>
    /// Driver-Side representation of a partitioned data set.
    /// </summary>
    public interface IPartitionedInputDataSet : IEnumerable<IPartitionDescriptor>
    {
        /// <summary>
        /// The number of partitions.
        /// </summary>
        int Count { get; }

        /// <summary>
        /// The Id of this dataset
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Get the PartitionDescriptor for the given Id.
        /// </summary>
        /// <param name="partitionId"></param>
        /// <returns>The IPartitionDescriptor found or null if none exists.</returns>
        IPartitionDescriptor GetPartitionDescriptorForId(string partitionId);
    }
}
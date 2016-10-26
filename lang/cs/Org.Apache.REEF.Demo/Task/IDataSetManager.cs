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

using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Demo.Task
{
    /// <summary>
    /// Evaluator-side interface to add and retrieve dataset partitions.
    /// </summary>
    [EvaluatorSide]
    public interface IDataSetManager
    {
        /// <summary>
        /// Fetch a partition using its id.
        /// </summary>
        IInputPartition<T> FetchPartition<T>(string dataSetId, string partitionId);

        /// <summary>
        /// Register a new partition to the local store.
        /// </summary>
        /// <param name="dataSetId">Id of the dataset this partition belongs to</param>
        /// <param name="partition">Id of the partition to register</param>
        /// <param name="reportToDriver">Whether to report the new partition to the driver or not</param>
        void AddLocalPartition<T>(string dataSetId, IInputPartition<T> partition, bool reportToDriver);
    }
}

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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Demo.Task
{
    /// <summary>
    /// Evaluator-side interface for reporting new partitions to the driver.
    /// </summary>
    [EvaluatorSide]
    [DefaultImplementation(typeof(PartitionReporter))]
    public interface IPartitionReporter
    {
        /// <summary>
        /// Report new partition to driver.
        /// </summary>
        /// <param name="dataSetId">Id of the dataset that the given partition belongs to</param>
        /// <param name="partitionId">Id of the partition to report</param>
        void ReportPartition(string dataSetId, string partitionId);
    }
}

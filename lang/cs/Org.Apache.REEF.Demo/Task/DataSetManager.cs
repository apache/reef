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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Demo.Task
{
    public sealed class DataSetManager : IDataSetManager
    {
        private readonly IPartitionReporter _partitionReporter;
        private readonly ConcurrentDictionary<string, ISet<IInputPartition<object>>> _localPartitions;

        [Inject]
        private DataSetManager(IPartitionReporter partitionReporter)
        {
            _partitionReporter = partitionReporter;
            _localPartitions = new ConcurrentDictionary<string, ISet<IInputPartition<object>>>();
        }

        public void AddLocalPartition<T>(string dataSetId, IInputPartition<T> inputPartition, bool reportToDriver)
        {
            var localPartitionsForDataSet = _localPartitions.GetOrAdd(dataSetId, _ => new HashSet<IInputPartition<object>>());
            localPartitionsForDataSet.Add((IInputPartition<object>)inputPartition);

            if (reportToDriver)
            {
                _partitionReporter.ReportPartition(dataSetId, inputPartition.Id);
            }
        }

        public IInputPartition<T> FetchPartition<T>(string dataSetId, string partitionId)
        {
            if (_localPartitions.ContainsKey(dataSetId))
            {
                return (IInputPartition<T>)_localPartitions[dataSetId].Single(partition => partition.Id.Equals(partitionId));
            }
            else
            {
                //// fetch from remote
                throw new NotImplementedException();
            }
        }
    }
}

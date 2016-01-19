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

using System.Collections;
using System.Collections.Generic;
using Org.Apache.REEF.IO.PartitionedData.Random.Parameters;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.PartitionedData.Random
{
    /// <summary>
    /// An implementation of IPartitionedDataSet that produces a given number of partitions containing a given number of random
    /// doubles.
    /// </summary>
    internal sealed class RandomInputDataSet : IPartitionedInputDataSet
    {
        private readonly Dictionary<string, IPartitionDescriptor> _partitions;

        [Inject]
        private RandomInputDataSet([Parameter(typeof(NumberOfPartitions))] int numberOfPartitions,
            [Parameter(typeof(NumberOfDoublesPerPartition))] int numberOfDoublesPerPartition)
        {
            _partitions = new Dictionary<string, IPartitionDescriptor>(numberOfPartitions);
            for (var i = 0; i < numberOfPartitions; ++i)
            {
                var id = "RandomInputPartition-" + i;
                _partitions[id] = new RandomInputPartitionDescriptor(id, numberOfDoublesPerPartition);
            }
        }

        public IEnumerator<IPartitionDescriptor> GetEnumerator()
        {
            return _partitions.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _partitions.Values.GetEnumerator();
        }

        public int Count
        {
            get { return _partitions.Count; }
        }

        public string Id
        {
            get { return "RandomDataSet"; }
        }

        public IPartitionDescriptor GetPartitionDescriptorForId(string partitionId)
        {
            return _partitions[partitionId];
        }
    }
}
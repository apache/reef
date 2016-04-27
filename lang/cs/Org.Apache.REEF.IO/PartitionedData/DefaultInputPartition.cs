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
using Org.Apache.REEF.IO.PartitionedData.Random.Parameters;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.PartitionedData
{
    /// <summary>
    /// A default implementation of <see cref="IInputPartition{T}"/> that utilizes 
    /// an inject <see cref="DataCache{T}"/>.
    /// </summary>
    public sealed class DefaultInputPartition<T> : IInputPartition<T>
    {
        private readonly DataCache<T> _dataCache;
        private readonly string _partitionId;

        [Inject]
        private DefaultInputPartition(
            [Parameter(typeof(PartitionId))] string partitionId,
            DataCache<T> dataCache)
        {
            _partitionId = partitionId;
            _dataCache = dataCache;
        }

        public string Id
        {
            get { return _partitionId; }
        }

        /// <summary>
        /// Caches to the designated <see cref="CacheLevel"/>.
        /// </summary>
        public CacheLevel Cache(CacheLevel level)
        {
            return _dataCache.Cache(level, true);
        }

        /// <summary>
        /// Materializes from based on the cache level of <see cref="DataCache{T}"/>,
        /// but does not cache the materialized data.
        /// </summary>
        public IEnumerable<T> GetPartitionHandle()
        {
            return _dataCache.Materialize();
        }
    }
}
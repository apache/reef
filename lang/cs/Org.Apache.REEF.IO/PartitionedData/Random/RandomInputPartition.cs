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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Org.Apache.REEF.IO.PartitionedData.Random.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.IO.PartitionedData.Random
{
    /// <summary>
    /// An implementation of IInputPartition that returns a configurable number of random doubles.
    /// </summary>
    [ThreadSafe]
    internal sealed class RandomInputPartition : IInputPartition<Stream>
    {
        private readonly object _lock = new object();
        private readonly string _id;
        private readonly int _numberOfDoubles;

        private Optional<byte[]> _randomData;

        [Inject]
        private RandomInputPartition(
            [Parameter(typeof(PartitionId))] string id,
            [Parameter(typeof(NumberOfDoublesPerPartition))] int numberOfDoubles)
        {
            _id = id;
            _numberOfDoubles = numberOfDoubles;
            _randomData = Optional<byte[]>.Empty();
        }

        public string Id
        {
            get { return _id; }
        }

        public CacheLevel Cache(CacheLevel level)
        {
            lock (_lock)
            {
                if (level > CacheLevel.InMemoryMaterialized)
                {
                    return CacheLevel.NotLocal;
                }

                if (_randomData.IsPresent())
                {
                    return CacheLevel.InMemoryMaterialized;
                }

                var random = new System.Random();
                var generatedData = new byte[_numberOfDoubles * sizeof(long)];
                for (var i = 0; i < _numberOfDoubles; ++i)
                {
                    var randomDouble = random.NextDouble();
                    var randomDoubleAsBytes = BitConverter.GetBytes(randomDouble);
                    Debug.Assert(randomDoubleAsBytes.Length == 8, "randomDoubleAsBytes.Length should be 8.");
                    for (var j = 0; j < sizeof(long); ++j)
                    {
                        var index = (i * 8) + j;
                        Debug.Assert(index < generatedData.Length, "Index should be less than _randomData.Length.");
                        generatedData[index] = randomDoubleAsBytes[j];
                    }
                }

                _randomData = Optional<byte[]>.Of(generatedData);
                return CacheLevel.InMemoryMaterialized;
            }
        }

        public IEnumerable<Stream> GetPartitionHandle()
        {
            lock (_lock)
            {
                if (!_randomData.IsPresent())
                {
                    Cache(CacheLevel.InMemoryMaterialized);
                }

                return new List<Stream> { new MemoryStream(_randomData.Value, false) };
            }
        }
    }
}
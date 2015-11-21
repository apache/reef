/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Diagnostics;
using System.IO;
using Org.Apache.REEF.IO.PartitionedData.Random.Parameters;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.PartitionedData.Random
{
    /// <summary>
    /// An implementation of IInputPartition that returns a configurable number of random doubles.
    /// </summary>
    internal sealed class RandomInputPartition : IInputPartition<Stream>
    {
        private readonly string _id;
        private readonly byte[] _randomData;

        [Inject]
        private RandomInputPartition([Parameter(typeof(PartitionId))] string id,
            [Parameter(typeof(NumberOfDoublesPerPartition))] int numberOfDoubles)
        {
            _id = id;
            _randomData = new byte[numberOfDoubles * 8];
            var random = new System.Random();

            for (var i = 0; i < numberOfDoubles; ++i)
            {
                var randomDouble = random.NextDouble();
                var randomDoubleAsBytes = BitConverter.GetBytes(randomDouble);
                Debug.Assert(randomDoubleAsBytes.Length == 8, "randomDoubleAsBytes.Length should be 8.");
                for (var j = 0; j < 8; ++j)
                {
                    var index = (i * 8) + j;
                    Debug.Assert(index < _randomData.Length, "Index should be less than _randomData.Length.");
                    _randomData[index] = randomDoubleAsBytes[j];
                }
            }
        }

        public string Id
        {
            get { return _id; }
        }

        public Stream GetPartitionHandle()
        {
            return new MemoryStream(_randomData, false);
        }
    }
}
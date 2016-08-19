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

namespace Org.Apache.REEF.Demo.Task
{
    internal sealed class TransformedPartition<T1, T2> : IInputPartition<T2>
    {
        private readonly string _id;
        private readonly IInputPartition<T1> _parentPartition;
        private readonly ITransform<T1, T2> _transform;
        private readonly object _cacheLock = new object();

        private bool _cached = false;
        private T2 _result;

        internal TransformedPartition(string id, IInputPartition<T1> parentPartition, ITransform<T1, T2> transform)
        {
            _id = id;
            _parentPartition = parentPartition;
            _transform = transform;
        }

        public string Id
        {
            get { return _id; }
        }

        public void Cache()
        {
            if (_cached)
            {
                return;
            }

            lock (_cacheLock)
            {
                if (_cached)
                {
                    return;
                }
    
                _result = _transform.Apply(_parentPartition.GetPartitionHandle());
                _cached = true;
            }
        }

        public T2 GetPartitionHandle()
        {
            if (_cached)
            {
                lock (_cacheLock)
                {
                    return _result;
                }
            }

            return _transform.Apply(_parentPartition.GetPartitionHandle());
        }
    }
}

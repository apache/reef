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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Demo.Driver
{
    /// <summary>
    /// Implementation of <see cref="IPartitionCollector"/> that doesn't consider different dataset ids.
    /// </summary>
    internal sealed class PartitionCollector : IPartitionCollector, IObserver<IContextMessage>
    {
        private readonly StringEnumerableCodec _stringEnumerableCodec;
        private readonly SynchronizedCollection<Tuple<string, string>> _contextAndPartitionIds;

        [Inject]
        private PartitionCollector(StringEnumerableCodec stringEnumerableCodec)
        {
            _stringEnumerableCodec = stringEnumerableCodec;
            _contextAndPartitionIds = new SynchronizedCollection<Tuple<string, string>>();
        }

        public void OnNext(IContextMessage msg)
        {
            string contextId = msg.MessageSourceId;
            foreach (string partitionId in _stringEnumerableCodec.Decode(msg.Message))
            {
                _contextAndPartitionIds.Add(new Tuple<string, string>(contextId, partitionId));
            }
        }

        public void OnCompleted()
        {
            throw new NotSupportedException();
        }

        public void OnError(Exception e)
        {
            throw new NotSupportedException();
        }

        public IEnumerable<Tuple<string, string>> ContextAndPartitionIdTuples
        {
            get
            {
                lock (_contextAndPartitionIds.SyncRoot)
                {
                    Tuple<string, string>[] tuples = new Tuple<string, string>[_contextAndPartitionIds.Count];
                    _contextAndPartitionIds.CopyTo(tuples, 0);
                    _contextAndPartitionIds.Clear();
                    return tuples;
                }
            }
        }
    }
}

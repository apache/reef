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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Demo.Task
{
    /// <summary>
    /// Implementation of <see cref="IPartitionReporter"/> that does not consider different dataset ids.
    /// </summary>
    public sealed class PartitionReporter : IPartitionReporter, IContextMessageSource
    {
        private readonly string _contextId;
        private readonly StringEnumerableCodec _stringEnumerableCodec; 
        private readonly SynchronizedCollection<string> _newPartitions;
        
        [Inject]
        private PartitionReporter([Parameter(typeof(ContextConfigurationOptions.ContextIdentifier))] string contextId,
                                  StringEnumerableCodec stringEnumerableCodec)
        {
            _contextId = contextId;
            _stringEnumerableCodec = stringEnumerableCodec;
            _newPartitions = new SynchronizedCollection<string>();
        }

        public void ReportPartition(string dataSetId, string partitionId)
        {
            _newPartitions.Add(partitionId);
        }

        public Optional<ContextMessage> Message
        {
            get
            {
                lock (_newPartitions.SyncRoot)
                {
                    if (_newPartitions.Count == 0)
                    {
                        return Optional<ContextMessage>.Empty();
                    }

                    var message = Optional<ContextMessage>.Of(ContextMessage.From(_contextId, _stringEnumerableCodec.Encode(_newPartitions)));
                    _newPartitions.Clear();
                    return message;
                }
            }
        }
    }
}

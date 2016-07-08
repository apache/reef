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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Demo.Examples
{
    public sealed class SampleTask : ITask
    {
        private readonly IDataSetManager _dataSetManager;
        private readonly ISet<string> _blockIds;

        [Inject]
        private SampleTask(IDataSetManager dataSetManager,
                           [Parameter(typeof(BlockIds))] ISet<string> blockIds)
        {
            _dataSetManager = dataSetManager;
            _blockIds = blockIds;
        }

        public byte[] Call(byte[] memento)
        {
            foreach (string blockId in _blockIds)
            {
                Block block = _dataSetManager.FetchBlock(blockId);
                // do something with the block data
                // decoding needed
                Console.WriteLine(block.Data);
            }

            return null;
        }

        public void Dispose()
        {
        }
    }
}

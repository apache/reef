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

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// It keeps the mapping between context id and partition descriptor id so that the context with the same id always bundled to the same partition data.
    /// </summary>
    internal sealed class PartitionDescriptorContextIdBundle
    {
        /// <summary>
        /// Create an object of PartitionDescriptorContextIdBunddle to maintain the relationship.
        /// If an evaluator failed and a new context is created, the same context id with the same partition data will be assigned to the new context
        /// </summary>
        /// <param name="partitionDescriptorId"></param>
        /// <param name="contextId"></param>
        internal PartitionDescriptorContextIdBundle(string partitionDescriptorId, string contextId)
        {
            PartitionDescriptorId = partitionDescriptorId;
            ContextId = contextId;
        }

        internal string PartitionDescriptorId { get; private set; }

        internal string ContextId { get; private set; }
    }
}

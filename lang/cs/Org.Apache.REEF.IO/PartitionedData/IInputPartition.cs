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

namespace Org.Apache.REEF.IO.PartitionedData
{
    /// <summary>
    /// Evaluator-Side representation of a data set partition.
    /// </summary>
    /// <typeparam name="T">Generic Type representing data pointer.
    /// For example, for data in local file it can be file pointer </typeparam>
    public interface IInputPartition<T> 
    {
        /// <summary>
        /// The id of the partition.
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Gives a pointer to the underlying partition.
        /// </summary>
        /// <returns>The pointer to the underlying partition</returns>
        T GetPartitionHandle();
    }
}
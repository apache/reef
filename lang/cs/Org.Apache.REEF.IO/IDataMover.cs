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
using System.IO;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.IO
{
    /// <summary>
    /// An interface that describes a contract for data movement between 
    /// different types of storage medium.
    /// </summary>
    [Unstable("0.15", "API contract may change.")]
    public interface IDataMover<T>
    {
        /// <summary>
        /// Fetches the data from remote and stores to disk.
        /// </summary>
        DirectoryInfo RemoteToDisk();

        /// <summary>
        /// Fetches the data from remote and puts in memory without deserialization.
        /// </summary>
        IEnumerable<MemoryStream> RemoteToMemory();

        /// <summary>
        /// Fetches the data from remote and deserializes it.
        /// </summary>
        IEnumerable<T> RemoteToMaterialized();

        /// <summary>
        /// Fetches the data from a directory on disk and puts in memory 
        /// without deserialization.
        /// </summary>
        /// <param name="info">The directory.</param>
        IEnumerable<MemoryStream> DiskToMemory(DirectoryInfo info);

        /// <summary>
        /// Fetches the data from a directory on disk and deserializes it.
        /// </summary>
        /// <param name="info"></param>
        IEnumerable<T> DiskToMaterialized(DirectoryInfo info);

        /// <summary>
        /// Deserializes a memory stream.
        /// </summary>
        /// <param name="streams">Memory streams to deserialize.</param>
        /// <returns></returns>
        IEnumerable<T> MemoryToMaterialized(IEnumerable<MemoryStream> streams);
    }
}
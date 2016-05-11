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
using System.Linq;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.IO
{
    /// <summary>
    /// A <see cref="IDataMover{T}"/> that uses <see cref="ISerializer{T}"/> to deserialize
    /// data from <see cref="MemoryStream"/>.
    /// </summary>
    [Unstable("0.15", "API contract may change.")]
    public abstract class SerializerBasedDataMover<T> : IDataMover<T>
    {
        private readonly ISerializer<T> _serializer;

        protected SerializerBasedDataMover(ISerializer<T> serializer)
        {
            _serializer = serializer;
        }

        protected ISerializer<T> Serializer
        {
            get { return _serializer; }
        }

        public abstract DirectoryInfo RemoteToDisk();

        public abstract IEnumerable<MemoryStream> RemoteToMemory();

        public abstract IEnumerable<T> RemoteToMaterialized();

        public abstract IEnumerable<MemoryStream> DiskToMemory(DirectoryInfo info);

        public abstract IEnumerable<T> DiskToMaterialized(DirectoryInfo info);

        public IEnumerable<T> MemoryToMaterialized(IEnumerable<MemoryStream> streams)
        {
            return StreamToMaterialized(streams);
        }

        /// <summary>
        /// Uses an <see cref="ISerializer{T}"/> to deserialize data from <see cref="IEnumerable{MemoryStream}"/>,
        /// where each stream is an object of type T.
        /// </summary>
        protected IEnumerable<T> StreamToMaterialized(IEnumerable<Stream> streams)
        {
            var deserialized = streams.Select(stream => _serializer.Deserialize(stream)).ToList();
            return deserialized.AsReadOnly();
        }
    }
}
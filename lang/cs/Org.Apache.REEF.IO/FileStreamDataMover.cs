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
    /// An abstract class inherits <see cref="SerializerBasedDataMover{T}"/> and models 
    /// each file as a <see cref="Stream"/> (and consequently each file an object 
    /// of type T.
    /// </summary>
    [Unstable("0.15", "API contract may change.")]
    public abstract class FileStreamDataMover<T> : SerializerBasedDataMover<T>
    {
        protected FileStreamDataMover(ISerializer<T> serializer) 
            : base(serializer)
        {
        }

        public override IEnumerable<MemoryStream> DiskToMemory(DirectoryInfo info)
        {
            foreach (var file in info.EnumerateFiles("*", SearchOption.AllDirectories))
            {
                var memStream = new MemoryStream();
                using (var fileStream = new FileStream(file.FullName, FileMode.Open))
                {
                    fileStream.CopyTo(memStream);
                }

                yield return memStream;
            }
        }

        public override IEnumerable<T> DiskToMaterialized(DirectoryInfo info)
        {
            var fileStreams = 
                info
                .EnumerateFiles("*", SearchOption.AllDirectories)
                .Select(file => new FileStream(file.FullName, FileMode.Open));

            return StreamToMaterialized(fileStreams);
        }
    }
}
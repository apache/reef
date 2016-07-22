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
using System.IO;
using System.Linq;
using Org.Apache.REEF.IO.PartitionedData.FileSystem;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Demo.Task
{
    public sealed class FileByteSerializer : IFileDeSerializer<byte[]>
    {
        [Inject]
        public FileByteSerializer()
        {
        }

        public byte[] Deserialize(ISet<string> filePaths)
        {
            IEnumerable<byte> data = new byte[0];

            foreach (string filePath in filePaths)
            {
                data = data.Concat(File.ReadAllBytes(filePath));
            }

            return data.ToArray();
        }
    }
}

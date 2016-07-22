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
using System.Linq;
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.IO.PartitionedData.FileSystem;
using Org.Apache.REEF.IO.PartitionedData.FileSystem.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Demo.Driver
{
    /// <summary>
    /// An implementation of IPartitionDescriptorFetcher that treats one file as one partition, regardless of the size.
    /// Internally uses IFileSystem to access the current file system.
    /// </summary>
    internal sealed class FileInputPartitionDescriptorFetcher : IPartitionDescriptorFetcher
    {
        private readonly IFileSystem _fileSystem;
        private readonly bool _copyToLocal;
        private readonly IConfiguration _serializerConf;

        [Inject]
        private FileInputPartitionDescriptorFetcher(IFileSystem fileSystem,
                                                    [Parameter(typeof(CopyToLocal))] bool copyToLocal)
        {
            _fileSystem = fileSystem;
            _copyToLocal = copyToLocal;
            _serializerConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IFileDeSerializer<byte[]>>.Class, GenericType<FileByteSerializer>.Class)
                .Build();
        }

        public IEnumerable<IPartitionDescriptor> GetPartitionDescriptors(Uri uri)
        {
            //// if uri points to a file: IFileSystem.GetChildren(uri) will return uri as it is.
            //// if it points to a folder: IFileSystem.GetChildren(uri) will return its children files and folders.
            //// in case there is a child folder, then THIS WILL NOT WORK.. we need to traverse through the folder structure recursively
            return _fileSystem.GetChildren(uri)
                .Select((childUri, index) => new FileInputPartitionDescriptor<byte[]>(
                    string.Format("Partition-{0}", index),
                    new List<string>
                    {
                        childUri.AbsoluteUri
                    },
                    _copyToLocal,
                    _serializerConf));
        }
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using Org.Apache.REEF.IO.PartitionedData.FileSystem.Parameters;
using Org.Apache.REEF.IO.PartitionedData.Random.Parameters;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.PartitionedData.FileSystem
{
    internal sealed class FilePartitionDescriptor<T> : IPartitionDescriptor
    {
        private readonly string _id;
        private readonly IList<string> _filePaths;
        private readonly IConfiguration _fileSystemPartitionConfig;

        internal FilePartitionDescriptor(string id, IList<string> filePaths, IConfiguration fileSystemPartitionConfig)
        {
            _id = id;
            _filePaths = filePaths;
            _fileSystemPartitionConfig = fileSystemPartitionConfig;
        }

        public string Id
        {
            get { return _id; }
        }

        /// <summary>
        /// This method is to provide Configurations required to inject the corresponding IPartition. 
        /// </summary>
        /// <returns></returns>
        public IConfiguration GetPartitionConfiguration()
        {
            var builder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IPartition<IEnumerable<T>>>.Class, GenericType<FilePartition<T>>.Class)
                .BindStringNamedParam<PartitionId>(_id);

            foreach (string p in _filePaths)
            {
                builder = builder.BindSetEntry<FilePathsInPartition, string>
                    (GenericType<FilePathsInPartition>.Class, p);
            }

            return Configurations.Merge(builder.Build(), _fileSystemPartitionConfig);
        }
    }
}

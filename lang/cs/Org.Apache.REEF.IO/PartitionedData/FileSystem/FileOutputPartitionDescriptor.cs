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

using Org.Apache.REEF.IO.PartitionedData.FileSystem.Parameters;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.PartitionedData.FileSystem
{
    internal sealed class FileOutputPartitionDescriptor<T> : IPartitionDescriptor
    {
        private readonly string _id;
        private readonly string _fileLocation;
        private readonly IConfiguration _filePartitionSerializerConfig;

        internal FileOutputPartitionDescriptor(string id, string fileLocation,
            IConfiguration filePartitionSerializerConfig)
        {
            _id = id;
            _fileLocation = fileLocation;
            _filePartitionSerializerConfig = filePartitionSerializerConfig;
        }

        /// <summary>
        /// Id of descriptor
        /// </summary>
        public string Id
        {
            get { return _id; }
        }

        /// <summary>
        /// This method is to provide Configurations required to inject the corresponding IOutputPartition. 
        /// </summary>
        /// <returns></returns>
        public IConfiguration GetPartitionConfiguration()
        {
            return TangFactory.GetTang().NewConfigurationBuilder(_filePartitionSerializerConfig)
                .BindImplementation(GenericType<IOutputPartition<T>>.Class,
                    GenericType<FileSystemOutputPartition<T>>.Class)
                .BindStringNamedParam<OutputPartitionId>(_id)
                .BindNamedParameter(typeof(FilePathForOutputPartition), _fileLocation)
                .Build();
        }
    }
}

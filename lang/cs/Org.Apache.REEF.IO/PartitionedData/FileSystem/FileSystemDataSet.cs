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

using System.Collections.Generic;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.PartitionedData.FileSystem.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.IO.PartitionedData.FileSystem
{
    /// <summary>
    /// This class is an implementation of IPartitionedDataSet for FileSystem.
    /// It requires the client to provide configuration for FileSerializer 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal sealed class FileSystemDataSet<T> : IPartitionedDataSet
    {
        private readonly Dictionary<string, IPartitionDescriptor> _partitions;
        private readonly int _count ; 
        
        [Inject]
        private FileSystemDataSet(
            [Parameter(typeof(AllFilePaths))] ISet<string> filePaths,
            IFileSystem fileSystem,
            [Parameter(typeof(FileSerializerConfigString))] string fileSerializerConfigString,
            AvroConfigurationSerializer avroConfigurationSerializer
            )
        {
            _count = filePaths.Count;

            _partitions = new Dictionary<string, IPartitionDescriptor>(_count);

            int i = 0;
            foreach (var path in filePaths)
            {
                var id = "FilePartition-" + i++;
                _partitions[id] = new FilePartitionDescriptor<T>(
                    id,
                    path,
                    Configurations.Merge(
                        FileSystemConfiguration(fileSystem),
                        avroConfigurationSerializer.FromString(fileSerializerConfigString))); 
            }
        }

        /// <summary>
        /// It returns the number of partions in the data set
        /// </summary>
        public int Count
        {
            get { return _count; }
        }

        /// <summary>
        /// It returns the id of the file system data set
        /// </summary>
        public string Id
        {
            get { return "FileSystemDataSet"; }
        }

        /// <summary>
        /// It returns PArtition Dewscriptor by given id
        /// </summary>
        /// <param name="partitionId"></param>
        /// <returns></returns>
        public IPartitionDescriptor GetPartitionDescriptorForId(string partitionId)
        {
            try
            {
                return _partitions[partitionId];
            }
            catch (KeyNotFoundException)
            {
                return null;
            }
        }

        /// <summary>
        /// Returns IEnumerator of IPartitionDescriptor
        /// </summary>
        /// <returns></returns>
        public IEnumerator<IPartitionDescriptor> GetEnumerator()
        {
            return _partitions.Values.GetEnumerator();
        }

        /// <summary>
        /// Returns IEnumerator of IPartitionDescriptor in the partitions
        /// </summary>
        /// <returns></returns>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _partitions.Values.GetEnumerator();
        }

        private IConfiguration FileSystemConfiguration(IFileSystem fileSystem)
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(typeof(IFileSystem), fileSystem.GetType())
                .Build();
        }
    }
}

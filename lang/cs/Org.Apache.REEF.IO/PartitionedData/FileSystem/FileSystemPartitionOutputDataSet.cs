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
using System.Collections;
using System.IO;
using System.Linq;
using Org.Apache.REEF.IO.PartitionedData.FileSystem.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;

namespace Org.Apache.REEF.IO.PartitionedData.FileSystem
{
    /// <summary>
    /// This class is an implementation of IPartitionedDataSet for FileSystem.
    /// It requires the client to provide configuration for FileSerializer 
    /// T is a type for data, e.g. a class like Row, or int, byte
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal sealed class FileSystemPartitionOutputDataSet<T> : IPartitionedOutputDataSet
    {
        private readonly Dictionary<string, IPartitionDescriptor> _partitions;
        private readonly int _count;
        private const string IdPrefix = "FileSystemOutputDataSet-";
        private readonly string _id;

        [Inject]
        private FileSystemPartitionOutputDataSet(
            [Parameter(typeof(FilePathsForOutputPartitions))] ISet<string> filePaths,
            [Parameter(typeof(FileSerializerConfigString))] string fileSerializerConfigString,
            AvroConfigurationSerializer avroConfigurationSerializer
            )
        {
            _count = filePaths.Count;
            _id = FormId(filePaths);
            _partitions = new Dictionary<string, IPartitionDescriptor>(_count);

            var fileSerializerConfig =
                avroConfigurationSerializer.FromString(fileSerializerConfigString);

            int i = 0;
            foreach (var path in filePaths)
            {
                var id = "FileOutputPartition-" + i++;
                _partitions[id] = new FileOutputPartitionDescriptor<T>(id, path, fileSerializerConfig);
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
            get { return _id; }
        }

        /// <summary>
        /// It returns Partition Descriptor by given id
        /// If id doesn't exists, it is callers responsibility to catch and handle exception
        /// </summary>
        /// <param name="partitionId"></param>
        /// <returns></returns>
        public IPartitionDescriptor GetPartitionDescriptorForId(string partitionId)
        {
            return _partitions[partitionId];
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
        IEnumerator IEnumerable.GetEnumerator()
        {
            return _partitions.Values.GetEnumerator();
        }

        private string FormId(ISet<string> filePaths)
        {
            string id = "";
            if (filePaths != null && filePaths.Count > 0)
            {
                var path = filePaths.First();

                FileInfo fInfo = new FileInfo(path);
                if (fInfo.Directory != null)
                {
                    id = fInfo.Directory.Name;
                }
            }
            return IdPrefix + id;
        }
    }
}

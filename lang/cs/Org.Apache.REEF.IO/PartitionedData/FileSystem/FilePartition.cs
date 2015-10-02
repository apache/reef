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
using System.IO;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.Hadoop;
using Org.Apache.REEF.IO.FileSystem.Local;
using Org.Apache.REEF.IO.PartitionedData.FileSystem.Parameters;
using Org.Apache.REEF.IO.PartitionedData.Random.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IO.PartitionedData.FileSystem
{
    internal sealed class FilePartition<T> : IPartition<IEnumerable<T>>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(FilePartition<T>));

        private readonly string _id;
        private readonly ISet<string> _filePaths;
        private readonly IFileSystem _fileSystem;
        private readonly IFileSerializer<T> _fileSerializer;
        private IList<string> localFileNames = new List<string>();

        [Inject]
        private FilePartition([Parameter(typeof(PartitionId))] string id,
            [Parameter(typeof(FilePathsInPartition))] ISet<string> filePaths,
            IFileSystem fileSystem,
            IFileSerializer<T> fileSerializer)
        {
            _id = id;
            _filePaths = filePaths;
            _fileSystem = fileSystem;
            _fileSerializer = fileSerializer;

            if (fileSystem is HadoopFileSystem)
            {
                Logger.Log(Level.Info, "+++++HadoopFileSystem");
            }

            if (fileSystem is LocalFileSystem)
            {
                Logger.Log(Level.Info, "!!!!!LocalFileSystem");
            }
        }

        public string Id
        {
            get { return _id; }
        }

        /// <summary>
        /// This method copy remote files to local and then deserialize the files.
        /// It returns the IEnumerble of T, the details is defined in the Deserialize() method 
        /// provided by the Serializer
        /// </summary>
        /// <returns></returns>
        public IEnumerable<T> GetPartitionHandle()
        {
            foreach (var f in _filePaths)
            {
                Logger.Log(Level.Info, "remoteFileName: " + f);
                localFileNames.Add(CopyFromRemote(f));
            }

            return _fileSerializer.Deserialize(localFileNames);
        }

        private string CopyFromRemote(string sourceFilePath)
        {
            Logger.Log(Level.Info, "GetUriPrefix: " + _fileSystem.UriPrefix);

            Uri sourceUri = new Uri(_fileSystem.UriPrefix + sourceFilePath);
            Logger.Log(Level.Info, "remoteUri: " + sourceUri);

            if (!_fileSystem.Exists(sourceUri))
            {
                throw new ApplicationException("Remote File does not exists.");
            }

            var localFilePath = Path.GetTempPath() + "-partition-" + DateTime.Now.ToString("yyyyMMddHHmmssfff");

            Logger.Log(Level.Info, "localFile: " + localFilePath);

            if (File.Exists(localFilePath))
            {
                File.Delete(localFilePath);
            }

            _fileSystem.CopyToLocal(sourceUri, localFilePath);
            if (File.Exists(localFilePath))
            {
                Logger.Log(Level.Info, "File CopyToLocal!");
            }
            else
            {
                Logger.Log(Level.Info, "File doesn't CopyToLocal!");               
            }

            return localFilePath;
        }

        ~FilePartition()
        {
            foreach (var fileName in localFileNames)
            {
                File.Delete(fileName);
            }
        }
    }
}

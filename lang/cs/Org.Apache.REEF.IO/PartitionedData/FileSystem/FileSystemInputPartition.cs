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
using System.Globalization;
using System.IO;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.PartitionedData.FileSystem.Parameters;
using Org.Apache.REEF.IO.PartitionedData.Random.Parameters;
using Org.Apache.REEF.IO.TempFileCreation;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IO.PartitionedData.FileSystem
{
    [ThreadSafe]
    internal sealed class FileSystemInputPartition<T> : IInputPartition<T>, IDisposable
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(FileSystemInputPartition<T>));

        private readonly string _id;
        private readonly IFileSystem _fileSystem;
        private readonly IFileDeSerializer<T> _fileSerializer;
        private readonly object _lock = new object();
        private readonly ITempFileCreator _tempFileCreator;
        private readonly ISet<string> _remoteFilePaths;
        private readonly bool _copyToLocal;
        private Optional<T> _data = Optional<T>.Empty();

        private Optional<ISet<string>> _localFiles;
        
        [Inject]
        private FileSystemInputPartition([Parameter(typeof(PartitionId))] string id,
            [Parameter(typeof(FilePathsInInputPartition))] ISet<string> remoteFilePaths,
            [Parameter(typeof(CopyToLocal))] bool copyToLocal,
            IFileSystem fileSystem,
            ITempFileCreator tempFileCreator,
            IFileDeSerializer<T> fileSerializer)
        {
            _id = id;
            _fileSystem = fileSystem;
            _fileSerializer = fileSerializer;
            _tempFileCreator = tempFileCreator;
            _remoteFilePaths = remoteFilePaths;
            _copyToLocal = copyToLocal;
            _localFiles = Optional<ISet<string>>.Empty();
        }

        public string Id
        {
            get { return _id; }
        }

        /// <summary>
        /// This method copies remote files to local if CopyToLocal is enabled, and then deserializes the files.
        /// Otherwise, this method assumes that the files are remote, and the injected IFileDeSerializer
        /// can handle the remote file system access.
        /// It caches the data reference returned from IFileDeSerializer.Deserialize() method. 
        /// </summary>
        public void Cache()
        {
            lock (_lock)
            {
                if (_copyToLocal)
                {
                    if (!_localFiles.IsPresent())
                    {
                        _localFiles = Optional<ISet<string>>.Of(Download());
                    }

                    // For now, assume IFileDeSerializer is local.
                    _data = Optional<T>.Of(_fileSerializer.Deserialize(_localFiles.Value));
                }
                else
                {
                    // For now, assume IFileDeSerializer is remote.
                    _data = Optional<T>.Of(_fileSerializer.Deserialize(_remoteFilePaths));
                }
            }
        }

        /// <summary>
        /// Downloads the remote file to local disk.
        /// </summary>
        private ISet<string> Download()
        {
            lock (_lock)
            {
                var set = new HashSet<string>();
                var localFileFolder = _tempFileCreator.CreateTempDirectory("-partition-");
                Logger.Log(Level.Info, "Local file temp folder: {0}", localFileFolder);

                foreach (var sourceFilePath in _remoteFilePaths)
                {
                    var sourceUri = _fileSystem.CreateUriForPath(sourceFilePath);
                    Logger.Log(Level.Verbose, "sourceUri {0}: ", sourceUri);

                    var localFilePath = Path.Combine(localFileFolder, Guid.NewGuid().ToString("N").Substring(0, 8));
                    set.Add(localFilePath);

                    Logger.Log(Level.Verbose, "LocalFilePath {0}: ", localFilePath);
                    if (File.Exists(localFilePath))
                    {
                        File.Delete(localFilePath);
                        Logger.Log(Level.Warning, "localFile {0} already exists, deleting it. ", localFilePath);
                    }

                    _fileSystem.CopyToLocal(sourceUri, localFilePath);
                }

                Logger.Log(Level.Info, "File downloading is completed");
                return set;
            }
        }

        /// <summary>
        /// If the data is not present, call cache() to get it. Then returns the data. 
        /// </summary>
        /// <returns></returns>
        public T GetPartitionHandle()
        {
            lock (_lock)
            {
                if (!_data.IsPresent())
                {
                    Cache();
                }
                return _data.Value;
            }
        }

        /// <summary>
        /// Implementation of the IDisposable.
        /// The caller should use using pattern and this method will be 
        /// called automatically when the object is out of scope.
        /// </summary>
        public void Dispose()
        {
            lock (_lock)
            {
                if (_localFiles.IsPresent())
                {
                    foreach (var fileName in _localFiles.Value)
                    {
                        File.Delete(fileName);
                    }
                }
            }
        }
    }
}

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
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IO.PartitionedData.FileSystem
{
    internal sealed class FileSystemInputPartition<T> : IInputPartition<T>, IDisposable
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(FileSystemInputPartition<T>));

        private readonly string _id;
        private readonly IFileSystem _fileSystem;
        private readonly IFileDeSerializer<T> _fileSerializer;
        private readonly ISet<string> _filePaths;
        private bool _isInitialized;
        private readonly bool _copyToLocal;
        private readonly object _lock = new object();
        private readonly ITempFileCreator _tempFileCreator;
        private readonly ISet<string> _localFiles = new HashSet<string>();

        [Inject]
        private FileSystemInputPartition([Parameter(typeof(PartitionId))] string id,
            [Parameter(typeof(FilePathsInInputPartition))] ISet<string> filePaths,
            [Parameter(typeof(CopyToLocal))] bool copyToLocal,
            IFileSystem fileSystem,
            ITempFileCreator tempFileCreator,
            IFileDeSerializer<T> fileSerializer)
        {
            _id = id;
            _fileSystem = fileSystem;
            _fileSerializer = fileSerializer;
            _filePaths = filePaths;
            _tempFileCreator = tempFileCreator;
            _isInitialized = false;
            _copyToLocal = copyToLocal;
        }

        public string Id
        {
            get { return _id; }
        }

        private void Initialize()
        {
            lock (_lock)
            {
                if (!_isInitialized)
                {
                    CopyFromRemote();
                    _isInitialized = true;
                }
            }
        }

        /// <summary>
        /// This method copy remote files to local and then deserialize the files.
        /// It returns the IEnumerble of T, the details is defined in the Deserialize() method 
        /// provided by the Serializer
        /// </summary>
        /// <returns></returns>
        public T GetPartitionHandle()
        {
            if (_copyToLocal)
            {
                if (!_isInitialized)
                {
                    Initialize();
                }

                return _fileSerializer.Deserialize(_localFiles);
            }
            return _fileSerializer.Deserialize(_filePaths);
        }

        private void CopyFromRemote()
        {
            string localFileFolder = _tempFileCreator.CreateTempDirectory("-partition-");
            Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "Local file temp folder: {0}", localFileFolder));

            foreach (var sourceFilePath in _filePaths)
            {
                Uri sourceUri = _fileSystem.CreateUriForPath(sourceFilePath);
                Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "sourceUri {0}: ", sourceUri));
                if (!_fileSystem.Exists(sourceUri))
                {
                    throw new FileNotFoundException(string.Format(CultureInfo.CurrentCulture,
                        "Remote File {0} does not exists.", sourceUri));
                }

                var localFilePath = localFileFolder + "\\" + Guid.NewGuid().ToString("N").Substring(0, 8);
                _localFiles.Add(localFilePath);

                Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "LocalFilePath {0}: ", localFilePath));
                if (File.Exists(localFilePath))
                {
                    File.Delete(localFilePath);
                    Logger.Log(Level.Warning, "localFile already exists, delete it: " + localFilePath);
                }

                _fileSystem.CopyToLocal(sourceUri, localFilePath);
                if (File.Exists(localFilePath))
                {
                    Logger.Log(Level.Info, 
                        string.Format(CultureInfo.CurrentCulture, "File {0} is Copied to local {1}.", sourceUri, localFilePath));
                }
                else
                {
                    string msg = string.Format(CultureInfo.CurrentCulture, 
                        "The IFilesystem completed the copy of `{0}` to `{1}`. But the file `{1}` does not exist.", sourceUri, localFilePath);
                    Exceptions.Throw(new FileLoadException(msg), msg, Logger);
                }
            }
        }

        /// <summary>
        /// Implementation of the IDisposable.
        /// The caller should use using pattern and this method will be 
        /// called automatically when the object is out of scope.
        /// </summary>
        public void Dispose()
        {
            _fileSerializer.Dispose();
            if (_localFiles.Count > 0)
            {
                foreach (var fileName in _localFiles)
                {
                    File.Delete(fileName);
                }
            }
        }
    }
}

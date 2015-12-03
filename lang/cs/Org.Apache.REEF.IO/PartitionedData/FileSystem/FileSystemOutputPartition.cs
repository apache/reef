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
using System.Globalization;
using System.IO;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.PartitionedData.FileSystem.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IO.PartitionedData.FileSystem
{
    internal sealed class FileSystemOutputPartition<T> : IOutputPartition<T>, IDisposable
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(FileSystemOutputPartition<T>));

        private readonly string _id;
        private readonly IFileSystem _fileSystem;
        private readonly IFileSerializer<T> _fileSerializer;
        private readonly string _remoteFilePath;
        private bool _isInitialized;
        private readonly object _lock = new object();
        private string _localFilePath;

        [Inject]
        private FileSystemOutputPartition([Parameter(typeof(OutputPartitionId))] string id,
            [Parameter(typeof(FilePathForOutputPartition))] string remoteFilePath,
            IFileSystem fileSystem,
            IFileSerializer<T> fileSerializer)
        {
            _id = id;
            _fileSystem = fileSystem;
            _fileSerializer = fileSerializer;
            _remoteFilePath = remoteFilePath;
            _isInitialized = false;
        }

        /// <summary>
        /// Id of the output partition
        /// </summary>
        public string Id
        {
            get { return _id; }
        }

        /// <summary>
        /// returns the output receiver that knows how to serialize and
        /// write result to the file
        /// </summary>
        /// <returns></returns>
        public T GetOutputReceiver()
        {
            if (!_isInitialized)
            {
                Initialize();
            }
            return _fileSerializer.Serializer(_localFilePath);
        }

        private void Initialize()
        {
            lock (_lock)
            {
                if (!_isInitialized)
                {
                    CreateTemporaryFile();
                    _isInitialized = true;
                }
            }
        }

        private void CopyToRemote()
        {
            Uri dstUri = new Uri(_fileSystem.UriPrefix + _remoteFilePath);
            Logger.Log(Level.Info, string.Format
                (CultureInfo.CurrentCulture, "dstUri {0}: ", dstUri));
            if (_fileSystem.Exists(dstUri))
            {
                throw new Exception(string.Format(CultureInfo.CurrentCulture,
                    "Remote File {0} already exists.", dstUri));
            }

            _fileSystem.CopyFromLocal(_localFilePath, dstUri);
            if (_fileSystem.Exists(dstUri))
            {
                Logger.Log(Level.Info, string.Format
                    (CultureInfo.CurrentCulture, "File {0} is Copied to local {1}.", _localFilePath, dstUri));
            }
            else
            {
                string msg = string.Format
                    (CultureInfo.CurrentCulture, "File {0} is NOT Copied to remote {1}.", _localFilePath, dstUri);
                Exceptions.Throw(new FileLoadException(), msg, Logger);
            }
        }

        private void CreateTemporaryFile()
        {
            string localFileFolder = Path.GetTempPath() + "-partition-" + Guid.NewGuid().ToString("N").Substring(0, 8);
            Directory.CreateDirectory(localFileFolder);
            _localFilePath = localFileFolder + "\\" + Guid.NewGuid().ToString("N").Substring(0, 8);
            Logger.Log(Level.Info, string.Format
                (CultureInfo.CurrentCulture, "localFilePath {0}: ", _localFilePath));
            if (File.Exists(_localFilePath))
            {
                File.Delete(_localFilePath);
                Logger.Log(Level.Warning, "localFile already exists, delete it: " + _localFilePath);
            }
        }

        /// <summary>
        /// Implementation of the IDisposable.
        /// The caller should use using pattern and this method will be 
        /// called automatically when the object is out of scope.
        /// File will be copied to remote location.
        /// </summary>
        public void Dispose()
        {
            CopyToRemote();
            if (_localFilePath != null)
            {
                File.Delete(_localFilePath);
            }
        }
    }
}

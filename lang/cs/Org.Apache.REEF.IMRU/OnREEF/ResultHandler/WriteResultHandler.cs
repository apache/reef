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
using System.IO;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.IMRU.OnREEF.ResultHandler
{
    /// <summary>
    /// Writes IMRU result from Update task to a file
    ///  </summary>
    /// <typeparam name="TResult"></typeparam>
    [Unstable("0.14", "This API will change after introducing proper API for output in REEF.IO")]
    public sealed class WriteResultHandler<TResult> : IIMRUResultHandler<TResult>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(WriteResultHandler<>));

        private readonly IStreamingCodec<TResult> _resultCodec;
        private readonly IFileSystem _fileSystem;
        private readonly string _remoteFileName;
        private readonly string _localFilename;

        [Inject]
        private WriteResultHandler(
            IStreamingCodec<TResult> resultCodec,
            IFileSystem fileSystem,
            [Parameter(typeof(ResultOutputLocation))] string fileName)
        {
            _resultCodec = resultCodec;
            _fileSystem = fileSystem;
            _remoteFileName = fileName;
            _localFilename = GenerateLocalFilename();
        }

        /// <summary>
        /// Specifies how to handle the IMRU results from the Update Task. Writes it to a location
        /// </summary>
        /// <param name="value">The result of IMRU from the UpdateTask</param>
        public void HandleResult(TResult value)
        {
            if (string.IsNullOrWhiteSpace(_remoteFileName))
            {
                return;
            }

            WriteOutput(value);
        }

        /// <summary>
        /// Handles what to do on completion
        /// In this case write to remote location
        /// </summary>
        public void Dispose()
        {
            if (string.IsNullOrWhiteSpace(_remoteFileName))
            {
                return;
            }

            Uri remoteUri = _fileSystem.CreateUriForPath(_remoteFileName);

            if (_fileSystem.Exists(remoteUri))
            {
                Utilities.Diagnostics.Exceptions.Throw(
                    new Exception(string.Format("Output Uri: {0} already exists", remoteUri)), Logger);
            }

            _fileSystem.CopyFromLocal(_localFilename, remoteUri);
        }

        private string GenerateLocalFilename()
        {
            string localFileFolder = Path.GetTempPath() + "-partition-" + Guid.NewGuid().ToString("N").Substring(0, 8);
            Directory.CreateDirectory(localFileFolder);
            var localFilePath = string.Format("{0}\\{1}", localFileFolder, Guid.NewGuid().ToString("N").Substring(0, 8));

            if (File.Exists(localFilePath))
            {
                File.Delete(localFilePath);
                Logger.Log(Level.Warning, "localFile for output already exists, deleting it: " + localFilePath);
            }

            return localFilePath;
        }

        private void WriteOutput(TResult result)
        {
            using (FileStream stream = new FileStream(_localFilename, FileMode.Append, FileAccess.Write))
            {
                StreamDataWriter writer = new StreamDataWriter(stream);
                _resultCodec.Write(result, writer);
            }
        }
    }
}

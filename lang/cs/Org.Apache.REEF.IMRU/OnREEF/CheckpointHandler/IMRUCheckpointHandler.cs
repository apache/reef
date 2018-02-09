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
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.IMRU.OnREEF.CheckpointHandler
{
    /// <summary>
    /// Default implementation of IIMRUCheckpointHandler
    /// </summary>
    public class IMRUCheckpointHandler : IIMRUCheckpointHandler
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(IMRUCheckpointHandler));

        private readonly IFileSystem _fileSystem;
        private readonly Uri _checkpointFileUrl;
        private readonly string _localFile;
        private readonly Uri _resultFileUrl;
        private readonly string _resultLocalFile;
        private readonly string _checkpointFilePath;
        private const string Done = "done";

        /// <summary>
        /// It is for storing and retrieving checkpoint data.
        /// </summary>
        /// <param name="checkpointFilePath">The file path where the checkpoint data will be stored.</param>
        /// <param name="fileSystem">File system to load/upload checkpoint data</param>
        [Inject]
        private IMRUCheckpointHandler(
            [Parameter(typeof(CheckpointFilePath))] string checkpointFilePath,
            IFileSystem fileSystem)
        {
            _fileSystem = fileSystem;
            _checkpointFilePath = checkpointFilePath;
            _localFile = "local" + Guid.NewGuid();
            _resultLocalFile = "local" + Guid.NewGuid();

            if (!string.IsNullOrEmpty(_checkpointFilePath))
            {
                _checkpointFileUrl = _fileSystem.CreateUriForPath(checkpointFilePath);
                _resultFileUrl = _fileSystem.CreateUriForPath(checkpointFilePath + "result");
            }
            Logger.Log(Level.Info, "State file path: {0}, localFile: {1}", checkpointFilePath, _localFile);
        }

        /// <summary>
        /// Save serialized checkpoint data to remote checkpoint file.
        /// </summary>
        /// <param name="taskState"></param>
        /// <param name="codec"></param>
        public void Persistent(ITaskState taskState, ICodec<ITaskState> codec)
        {
            var data = codec.Encode(taskState);
            File.WriteAllBytes(_localFile, data);

            if (!string.IsNullOrEmpty(_checkpointFilePath))
            {
                if (_fileSystem.Exists(_checkpointFileUrl))
                {
                    _fileSystem.Delete(_checkpointFileUrl);
                }

                _fileSystem.CopyFromLocal(_localFile, _checkpointFileUrl);
            }
        }

        /// <summary>
        /// Read checkpoint data and deserialize it into ITaskState object.
        /// </summary>
        /// <param name="codec"></param>
        /// <returns></returns>
        public ITaskState Restore(ICodec<ITaskState> codec)
        {
            if (!string.IsNullOrEmpty(_checkpointFilePath) && _fileSystem.Exists(_checkpointFileUrl))
            {
                _fileSystem.CopyToLocal(_checkpointFileUrl, _localFile);
                var currentState = File.ReadAllBytes(_localFile);
                return codec.Decode(currentState);
            }
            return null;
        }

        public void SetResult()
        {
            Logger.Log(Level.Info, "SetResult to file {0}", _resultFileUrl);

            if (!string.IsNullOrEmpty(_checkpointFilePath) && !_fileSystem.Exists(_resultFileUrl))
            {
                File.WriteAllText(_resultLocalFile, Done);
                _fileSystem.CopyFromLocal(_resultLocalFile, _resultFileUrl);
            }
        }

        public bool GetResult()
        {
            if (!string.IsNullOrEmpty(_checkpointFilePath) && _fileSystem.Exists(_resultFileUrl))
            {
                _fileSystem.CopyToLocal(_resultFileUrl, _resultLocalFile);
                var result = File.ReadAllText(_resultLocalFile);
                Logger.Log(Level.Info, "GetResult: {0}", result);
                return Done.Equals(result);
            }
            return false;
        }

        /// <summary>
        /// Delete checkpoint file if it exists. It should be only called once at begining of task initialization.  
        /// </summary>
        public void Reset()
        {
            if (!string.IsNullOrEmpty(_checkpointFilePath) && _fileSystem.Exists(_checkpointFileUrl))
            {
                _fileSystem.Delete(_checkpointFileUrl);
            }
            if (!string.IsNullOrEmpty(_checkpointFilePath) && _fileSystem.Exists(_resultFileUrl))
            {
                _fileSystem.Delete(_resultFileUrl);
            }
        }
    }
}
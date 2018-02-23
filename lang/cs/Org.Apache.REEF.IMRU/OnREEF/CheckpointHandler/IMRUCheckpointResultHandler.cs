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
using System.Linq;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.CheckpointHandler
{
    /// <summary>
    /// Internal class that handles checkpoint for result flag.
    /// </summary>
    internal class IMRUCheckpointResultHandler : IIMRUCheckpointResultHandler
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(IIMRUCheckpointResultHandler));

        private const string RresultDir = "RresultDir";
        private const string RresultFile = "result.txt";
        private const string Done = "done";

        private readonly IFileSystem _fileSystem;
        private readonly Uri _resultFileUrl;
        private readonly string _checkpointFilePath;

        /// <summary>
        /// It is for storing and retrieving checkpoint result flag.
        /// </summary>
        /// <param name="checkpointFilePath">The file path where the checkpoint data will be stored.</param>
        /// <param name="fileSystem">File system to load/upload checkpoint data</param>
        [Inject]
        private IMRUCheckpointResultHandler(
            [Parameter(typeof(CheckpointFilePath))] string checkpointFilePath,
            IFileSystem fileSystem)
        {
            _fileSystem = fileSystem;
            _checkpointFilePath = checkpointFilePath;

            if (!string.IsNullOrEmpty(_checkpointFilePath))
            {
                string resultFile = Path.Combine(_checkpointFilePath, RresultDir, RresultFile);
                _resultFileUrl = _fileSystem.CreateUriForPath(resultFile);
            }
            Logger.Log(Level.Info, "############ state file path: {0}", checkpointFilePath);
        }

        /// <summary>
        /// Set flag to show result is already written.
        /// </summary>
        public void SetResult()
        {
            if (!string.IsNullOrEmpty(_checkpointFilePath) && !_fileSystem.Exists(_resultFileUrl))
            {
                var resultLocalFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N").Substring(0, 4));
                File.WriteAllText(resultLocalFile, Done);
                _fileSystem.CopyFromLocal(resultLocalFile, _resultFileUrl);
                File.Delete(resultLocalFile);
            }
        }

        /// <summary>
        /// Retrieve the result flag.
        /// </summary>
        /// <returns></returns>
        public bool GetResult()
        {
            if (!string.IsNullOrEmpty(_checkpointFilePath) && _fileSystem.Exists(_resultFileUrl))
            {
                var resultLocalFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N").Substring(0, 4));
                _fileSystem.CopyToLocal(_resultFileUrl, resultLocalFile);
                var result = File.ReadAllText(resultLocalFile);
                Logger.Log(Level.Info, "GetResult: {0}", result);
                return Done.Equals(result);
            }
            return false;
        }

        /// <summary>
        /// Clear checkpoint files in checkpointFilePath. It should be only called once for the entire job.  
        /// </summary>
        public void Clear()
        {
            if (!string.IsNullOrEmpty(_checkpointFilePath))
            {
                var filesToRemove = _fileSystem.GetChildren(_fileSystem.CreateUriForPath(_checkpointFilePath));
                var streamToRemove = filesToRemove.Where(f => IsStateFile(f.AbsolutePath));
                foreach (var stream in streamToRemove)
                {
                    _fileSystem.Delete(stream);
                }
            }
        }

        private bool IsStateFile(string path)
        {
            return path.EndsWith(IMRUCheckpointHandler.StateFileExt) || path.EndsWith(IMRUCheckpointHandler.FlagFileExt);
        }
    }
}
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
        private readonly string _checkpointFilePath;
        private const string StateDir = "StateDir";
        private const string StateFile = "StateFile";
        private const string FlagFile = "FlagFile";
        public const string StateFileExt = ".bin";
        public const string FlagFileExt = ".txt";

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

            Logger.Log(Level.Info, "State file path: {0}", checkpointFilePath);
        }

        /// <summary>
        /// Save serialized checkpoint data to remote checkpoint file.
        /// </summary>
        /// <param name="taskState"></param>
        /// <param name="codec"></param>
        public void Persistent(ITaskState taskState, ICodec<ITaskState> codec)
        {
            var localStateFile = Path.GetTempPath() + Guid.NewGuid().ToString("N").Substring(0, 4);
            var localFlagfile = Path.GetTempPath() + Guid.NewGuid().ToString("N").Substring(0, 4);

            string tick = DateTime.Now.Ticks.ToString();
            string stateFileDir = Path.Combine(_checkpointFilePath, StateDir + tick);
            string remoteStateFileName = Path.Combine(stateFileDir, StateFile + tick + StateFileExt);
            string remoteFlagFileName = Path.Combine(stateFileDir, FlagFile + tick + FlagFileExt);

            var stateFileUri = _fileSystem.CreateUriForPath(remoteStateFileName);
            var flagFileUri = _fileSystem.CreateUriForPath(remoteFlagFileName);

            var data = codec.Encode(taskState);
            File.WriteAllBytes(localStateFile, data);
            File.WriteAllText(localFlagfile, remoteStateFileName);

            _fileSystem.CopyFromLocal(localStateFile, stateFileUri);
            _fileSystem.CopyFromLocal(localFlagfile, flagFileUri);

            File.Delete(localStateFile);
            File.Delete(localFlagfile);
        }

        /// <summary>
        /// Read checkpoint data and deserialize it into ITaskState object.
        /// </summary>
        /// <param name="codec"></param>
        /// <returns></returns>
        public ITaskState Restore(ICodec<ITaskState> codec)
        {
            if (!string.IsNullOrEmpty(_checkpointFilePath))
            {
                var files = _fileSystem.GetChildren(_fileSystem.CreateUriForPath(_checkpointFilePath));
                if (files != null)
                {
                    var flagFiles = files.Where(f => f.AbsolutePath.Contains(FlagFile));
                    var uris = flagFiles.OrderByDescending(ff => _fileSystem.GetFileStatus(ff).ModificationTime);

                    Uri latestFlagFile = uris.FirstOrDefault();
                    if (latestFlagFile != null)
                    {
                        var localLatestFlagfile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N").Substring(0, 4));
                        var localLatestStatefile = Path.Combine(Path.GetTempPath() + Guid.NewGuid().ToString("N").Substring(0, 4));

                        _fileSystem.CopyToLocal(latestFlagFile, localLatestFlagfile);
                        string latestStateFile = File.ReadAllText(localLatestFlagfile);
                        Logger.Log(Level.Info, "latestStateFile -- : {0}", latestStateFile);
                        var latestStateFileUri = _fileSystem.CreateUriForPath(latestStateFile);
                        _fileSystem.CopyToLocal(latestStateFileUri, localLatestStatefile);
                        var currentState = File.ReadAllBytes(localLatestStatefile);

                        File.Delete(localLatestFlagfile);
                        File.Delete(localLatestStatefile);

                        return codec.Decode(currentState);
                    }
                }
            }
            return null;
        }
    }
}
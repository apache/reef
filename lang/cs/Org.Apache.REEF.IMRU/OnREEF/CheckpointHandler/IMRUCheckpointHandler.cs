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
using System.IO;
using System.Linq;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.TempFileCreation;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.IMRU.OnREEF.CheckpointHandler
{
    /// <summary>
    /// Default implementation of IIMRUCheckpointHandler
    /// </summary>
    public sealed class IMRUCheckpointHandler : IIMRUCheckpointHandler
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(IMRUCheckpointHandler));

        private readonly IFileSystem _fileSystem;
        private readonly ICodec<ITaskState> _stateCodec;
        private readonly string _checkpointFilePath;
        private readonly Uri _checkpointFileUri;
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
        /// <param name="stateCodec">Codec that is used for decoding and encoding for State object.</param>
        [Inject]
        private IMRUCheckpointHandler(
            [Parameter(typeof(CheckpointFilePath))] string checkpointFilePath,
            ICodec<ITaskState> stateCodec,
            IFileSystem fileSystem)
        {
            _fileSystem = fileSystem;
            _stateCodec = stateCodec;
            _checkpointFilePath = checkpointFilePath;
            _checkpointFileUri = _fileSystem.CreateUriForPath(_checkpointFilePath);
            Logger.Log(Level.Info, "State file path: {0}", checkpointFilePath);
        }

        /// <summary>
        /// Save serialized checkpoint data to remote checkpoint file.
        /// </summary>
        /// <param name="taskState"></param>
        public void Persist(ITaskState taskState)
        {
            var localStateFile = TangFactory.GetTang().NewInjector().GetInstance<ITempFileCreator>().GetTempFileName("statefile", string.Empty);
            var localFlagfile = TangFactory.GetTang().NewInjector().GetInstance<ITempFileCreator>().GetTempFileName("flagfile", string.Empty);

            string tick = DateTime.Now.Ticks.ToString();
            string stateFileDir = Path.Combine(_checkpointFilePath, StateDir + tick);
            string remoteStateFileName = Path.Combine(stateFileDir, StateFile + tick + StateFileExt);
            string remoteFlagFileName = Path.Combine(stateFileDir, FlagFile + tick + FlagFileExt);

            var stateFileUri = _fileSystem.CreateUriForPath(remoteStateFileName);
            var flagFileUri = _fileSystem.CreateUriForPath(remoteFlagFileName);

            var data = _stateCodec.Encode(taskState);
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
        /// <returns></returns>
        public ITaskState Restore()
        {
            if (!string.IsNullOrEmpty(_checkpointFilePath))
            {
                var files = _fileSystem.GetChildren(_checkpointFileUri);
                if (files != null)
                {
                    var flagFiles = files.Where(f => f.AbsolutePath.Contains(FlagFile));
                    var uris = flagFiles.OrderByDescending(ff => _fileSystem.GetFileStatus(ff).ModificationTime).ToList();

                    return Restore(uris);
                }
            }
            return null;
        }

        private ITaskState Restore(IList<Uri> uris)
        {
            Uri latestFlagFile = uris.FirstOrDefault();
            if (latestFlagFile != null)
            {
                var localLatestStatefile = TangFactory.GetTang().NewInjector().GetInstance<ITempFileCreator>().GetTempFileName("statefile", string.Empty);
                var localLatestFlagfile = TangFactory.GetTang().NewInjector().GetInstance<ITempFileCreator>().GetTempFileName("flagfile", string.Empty);

                _fileSystem.CopyToLocal(latestFlagFile, localLatestFlagfile);

                try
                {
                    string latestStateFile = File.ReadAllText(localLatestFlagfile);
                    Logger.Log(Level.Info, "latestStateFile -- : {0}", latestStateFile);
                    var latestStateFileUri = _fileSystem.CreateUriForPath(latestStateFile);
                    _fileSystem.CopyToLocal(latestStateFileUri, localLatestStatefile);
                    var currentState = File.ReadAllBytes(localLatestStatefile);
                    return _stateCodec.Decode(currentState);
                }
                catch (Exception e)
                {
                    Logger.Log(Level.Info, "Exception in restoring from state file. Possible file corruption {0}", e);
                    uris.RemoveAt(0);
                    return Restore(uris);
                }
                finally
                {
                    File.Delete(localLatestFlagfile);
                    File.Delete(localLatestStatefile);
                }
            }
            return null;
        }
    }
}
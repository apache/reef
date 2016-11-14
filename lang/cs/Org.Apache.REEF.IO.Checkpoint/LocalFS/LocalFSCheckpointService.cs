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
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.Checkpoint.LocalFS
{
    class LocalFSCheckpointService : ICheckpointService
    {
        private readonly string _basePath;
        private readonly ICheckpointNamingService _namingPolicy;

        [Inject]
        public LocalFSCheckpointService(
            [Parameter(Value = typeof(CheckpointPath))] string path,
            ICheckpointNamingService namingPolicy)
        {
            _basePath = path;
            _namingPolicy = namingPolicy;
        }

        public ICheckpointWriter Create()
        {
            string name = _namingPolicy.GetNewName();
            
            return new LocalFSCheckpointWriter(_basePath + name, File.Create(_basePath + name));
        }

        public ICheckpointID Commit(ICheckpointWriter writer)
        {
            if (writer.IsOpen())
            {
                writer.Close();
            }

            string dst = (writer as LocalFSCheckpointWriter).GetDestination();
            return new LocalFSCheckpointID(dst);
        }

        public void Abort(ICheckpointWriter writer)
        {
            if (writer.IsOpen())
            {
                writer.Close();
            }

            string dst = (writer as LocalFSCheckpointWriter).GetDestination();
            File.Delete(dst);
        }

        // TODO Complete "Checkpointing task" in REEF.Example Tasks
        // TODO test the example
        // TODO fix a conflict in .sln file arising from the #1196 merge
        // TODO find out if a nuget is needed
        public void Delete(ICheckpointID checkpointId)
        {
            if (!(checkpointId is LocalFSCheckpointID))
            {
                throw new ArgumentException("Mismatched checkpoint id type.Expected FSCheckpointID, but actually got " + checkpointId.GetType());
            }

            string path = ((LocalFSCheckpointID)checkpointId).GetPath();
            File.Delete(path);
        }

        public ICheckpointReader Open(ICheckpointID checkpointId)
        {
            if (!(checkpointId is LocalFSCheckpointID))
            {
                throw new ArgumentException("Mismatched checkpoint id type.Expected FSCheckpointID, but actually got " + checkpointId.GetType());
            }

            return new LocalFSCheckpointReader(((LocalFSCheckpointID)checkpointId).GetPath());
        }

        [NamedParameter("Path to checkpoint for local checkpoint service.", "checkpoint_localfs_path")]
        public class CheckpointPath : Name<string>
        {
        }

        private class LocalFSCheckpointWriter : ICheckpointWriter
        {
            private readonly string _finalDist;
            private readonly BinaryWriter _out;
            private bool _isOpen;

            public LocalFSCheckpointWriter(string finalDist, Stream output)
            {
                _finalDist = finalDist;
                _out = new BinaryWriter(output);
                _isOpen = true;
            }

            public void Write(ref byte[] buffer)
            {
                _out.Write(buffer);
            }

            public bool IsOpen()
            {
                return _isOpen; 
            }

            public void Close()
            {
                _isOpen = false;
                _out.Close();
            }

            public string GetDestination()
            {
                return _finalDist;
            }
        }

        private class LocalFSCheckpointReader : ICheckpointReader
        {
            private readonly BinaryReader _reader;

            public LocalFSCheckpointReader(string path)
            {
                _reader = new BinaryReader(new FileStream(path, FileMode.Open));
            }

            public int Read(ref byte[] buffer, int index, int count)
            {
                return _reader.Read(buffer, index, count);
            }
        }
    }
}

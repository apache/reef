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
// software distributed under the License is distributed on anAssert.Equal
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.CheckpointHandler;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.TempFileCreation;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Wake.Remote;
using Xunit;

namespace Org.Apache.REEF.IMRU.Tests
{
    public class TestCheckpointHandler
    {
        /// <summary>
        /// Test persist and restore.
        /// </summary>
        [Fact]
        public void TestPersistent()
        {
            var injector = TangFactory.GetTang().NewInjector(BuildCheckpointConfig());
            var checkpointHandler = injector.GetInstance<IIMRUCheckpointHandler>();
            var checkpointResultHandler = injector.GetInstance<API.IIMRUCheckpointResultHandler>();

            // Test to clean a non existing folder
            checkpointResultHandler.Clear();

            var taskState = (TestTaskState)TangFactory.GetTang().NewInjector(TaskStateConfiguration()).GetInstance<ITaskState>();
            taskState.Iterations = 5;
            checkpointHandler.Persist(taskState);
            taskState.Iterations = 10;
            checkpointHandler.Persist(taskState);

            var state = checkpointHandler.Restore();
            var testTaskState = (TestTaskState)state;
            Assert.Equal(testTaskState.Iterations, 10);

            checkpointResultHandler.MarkResulHandled();
            var r = checkpointResultHandler.IsResultHandled();
            Assert.True(r);

            checkpointResultHandler.Clear();
        }

        /// <summary>
        /// Test last state file corrupted during the restore.
        /// </summary>
        [Fact]
        public void TestLastFileCorrupted()
        {
            var filePath = TangFactory.GetTang().NewInjector().GetInstance<ITempFileCreator>().CreateTempDirectory("statefiles", string.Empty);

            var config = CheckpointConfigurationBuilder.ConfigurationModule
                .Set(CheckpointConfigurationBuilder.CheckpointFilePath, filePath)
                .Set(CheckpointConfigurationBuilder.TaskStateCodec, GenericType<TestTaskStateCodec>.Class)
                .Build();

            var injector = TangFactory.GetTang().NewInjector(config);
            var checkpointHandler = injector.GetInstance<IIMRUCheckpointHandler>();
            var checkpointResultHandler = injector.GetInstance<API.IIMRUCheckpointResultHandler>();
            var fileSystem = injector.GetInstance<IFileSystem>();

            // Test to clean a non existing folder
            checkpointResultHandler.Clear();

            // Write two state files
            var taskState = (TestTaskState)TangFactory.GetTang().NewInjector(TaskStateConfiguration()).GetInstance<ITaskState>();
            taskState.Iterations = 5;
            checkpointHandler.Persist(taskState);
            taskState.Iterations = 10;
            checkpointHandler.Persist(taskState);

            // Delete the last state file
            var files = fileSystem.GetChildren(fileSystem.CreateUriForPath(filePath));
            var flagFiles = files.Where(f => f.AbsolutePath.Contains("FlagFile"));
            var uris = flagFiles.OrderByDescending(ff => fileSystem.GetFileStatus(ff).ModificationTime).ToList();
            Uri latestFlagFile = uris.FirstOrDefault();
            var localLatestFlagfile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N").Substring(0, 4));
            fileSystem.CopyToLocal(latestFlagFile, localLatestFlagfile);
            string latestStateFile = File.ReadAllText(localLatestFlagfile);
            fileSystem.Delete(fileSystem.CreateUriForPath(latestStateFile));

            var state = checkpointHandler.Restore();
            var testTaskState = (TestTaskState)state;
            Assert.Equal(testTaskState.Iterations, 5);
        }

        protected IConfiguration BuildCheckpointConfig()
        {
            var filePath = TangFactory.GetTang().NewInjector().GetInstance<ITempFileCreator>().CreateTempDirectory("statefiles", string.Empty);

            return CheckpointConfigurationBuilder.ConfigurationModule
                .Set(CheckpointConfigurationBuilder.CheckpointFilePath, filePath)
                .Set(CheckpointConfigurationBuilder.TaskStateCodec, GenericType<TestTaskStateCodec>.Class)
                .Build();
        }

        private IConfiguration TaskStateConfiguration()
        {
            return TangFactory.GetTang()
                .NewConfigurationBuilder()
                .BindImplementation(GenericType<ITaskState>.Class, GenericType<TestTaskState>.Class)
                .Build();
        }
    }

    [DataContract]
    internal sealed class TestTaskState : ITaskState
    {
        [DataMember]
        internal int Iterations { get; set; }

        [Inject]
        [JsonConstructor]
        private TestTaskState()
        {
        }

        internal void Update(TestTaskState taskState)
        {
            Iterations = taskState.Iterations;
        }
    }

    internal class TestTaskStateCodec : ICodec<ITaskState>
    {
        [Inject]
        TestTaskStateCodec()
        {
        }

        public ITaskState Decode(byte[] data)
        {
            var str = ByteUtilities.ByteArraysToString(data);
            return JsonConvert.DeserializeObject<TestTaskState>(str);
        }

        public byte[] Encode(ITaskState taskState)
        {
            var state = JsonConvert.SerializeObject(taskState);
            return ByteUtilities.StringToByteArrays(state);
        }
    }
}
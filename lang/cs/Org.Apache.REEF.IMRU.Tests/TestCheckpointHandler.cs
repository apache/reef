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
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.CheckpointHandler;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
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
        [Fact]
        public void TestPersistent()
        {
            var injector = TangFactory.GetTang().NewInjector(BuildCheckpointConfig());
            var checkpointHandler = injector.GetInstance<IIMRUCheckpointHandler>();
            var checkpointResultHandler = injector.GetInstance<API.IIMRUCheckpointResultHandler>();
            var codec = injector.GetInstance<ICodec<ITaskState>>();

            // Test to clean a non existing folder
            checkpointResultHandler.Clear();

            var taskState = (TestTaskState)TangFactory.GetTang().NewInjector(TaskStateConfiguration()).GetInstance<ITaskState>();
            taskState.Iterations = 5;
            checkpointHandler.Persistent(taskState, codec);
            taskState.Iterations = 10;
            checkpointHandler.Persistent(taskState, codec);

            var state = checkpointHandler.Restore(codec);
            var testTaskState = (TestTaskState)state;
            Assert.Equal(testTaskState.Iterations, 10);

            checkpointResultHandler.SetResult();
            var r = checkpointResultHandler.GetResult();
            Assert.True(r);

            checkpointResultHandler.Clear();
        }

        protected IConfiguration BuildCheckpointConfig()
        {
            var filePath = Path.Combine(Path.GetTempPath(), "teststatepath" + Guid.NewGuid().ToString("N").Substring(0, 4));

            return CheckpointConfigurationModule.ConfigurationModule
                .Set(CheckpointConfigurationModule.CheckpointFile, filePath)
                .Set(CheckpointConfigurationModule.TaskStateCodec, GenericType<TestTaskStateCodec>.Class)
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
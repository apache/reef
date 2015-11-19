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
using System.CodeDom;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.IMRU.OnREEF;
using Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.StreamingCodec;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;

namespace Org.Apache.REEF.IMRU.Tests
{
    [TestClass]
    public class MapInputWithControlMessageTests
    {
        /// <summary>
        /// Tests the codec for TMapInputWithControlMessage
        /// </summary>
        [TestMethod]
        public void TestMapInputWithControlMessageCodec()
        {
            float[] baseMessage = { 0, 1 };

            var config = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IStreamingCodec<float[]>>.Class,
                    GenericType<FloatArrayStreamingCodec>.Class)
                .Build();

            IStreamingCodec<MapInputWithControlMessage<float[]>> codec =
                TangFactory.GetTang().NewInjector(config).GetInstance<MapInputWithControlMessageCodec<float[]>>();

            MemoryStream stream = new MemoryStream();
            IDataWriter writer = new StreamDataWriter(stream);

            codec.Write(new MapInputWithControlMessage<float[]>(baseMessage, MapControlMessage.AnotherRound), writer);
            codec.Write(new MapInputWithControlMessage<float[]>(MapControlMessage.Stop), writer);

            stream.Position = 0;
            IDataReader reader = new StreamDataReader(stream);

            var message1 = codec.Read(reader);
            var message2 = codec.Read(reader);

            Assert.AreEqual(message1.Message[0], baseMessage[0]);
            Assert.AreEqual(message1.Message[1], baseMessage[1]);
            Assert.IsNull(message2.Message);
            Assert.AreEqual(message1.ControlMessage, MapControlMessage.AnotherRound);
            Assert.AreEqual(message2.ControlMessage, MapControlMessage.Stop);
        }

        /// <summary>
        /// Tests the pipelining Data converter for TMapInputWithControlMessage
        /// </summary>
        [TestMethod]
        public void TestMapInputPipelining()
        {
            int chunkSize = 2;

            var config = TangFactory.GetTang().NewConfigurationBuilder(
                PipelineDataConverterConfiguration<int[]>.Conf
                    .Set(PipelineDataConverterConfiguration<int[]>.DataConverter,
                        GenericType<PipelineIntDataConverter>.Class)
                    .Build()).BindNamedParameter<ChunkSize, int>(
                        GenericType<ChunkSize>.Class,
                        chunkSize.ToString(CultureInfo.InvariantCulture)).Build();

            IPipelineDataConverter<MapInputWithControlMessage<int[]>> dataConverter =
                TangFactory.GetTang()
                    .NewInjector(config)
                    .GetInstance<MapInputwithControlMessagePipelineDataConverter<int[]>>();

            int[] baseMessage = { 1, 2, 3 };

            var chunks1 = dataConverter.PipelineMessage(new MapInputWithControlMessage<int[]>(baseMessage,
                MapControlMessage.AnotherRound));

            var chunks2 = dataConverter.PipelineMessage(new MapInputWithControlMessage<int[]>(MapControlMessage.Stop));

            Assert.AreEqual(chunks1.Count, 2);
            Assert.IsTrue(chunks1[0].Data.Message.Length == 2);
            Assert.IsTrue(chunks1[1].Data.Message.Length == 1);
            Assert.AreEqual(chunks1[0].Data.Message[0], baseMessage[0]);
            Assert.AreEqual(chunks1[0].Data.Message[1], baseMessage[1]);
            Assert.AreEqual(chunks1[1].Data.Message[0], baseMessage[2]);
            Assert.AreEqual(chunks1[0].Data.ControlMessage, MapControlMessage.AnotherRound);
            Assert.AreEqual(chunks1[1].Data.ControlMessage, MapControlMessage.AnotherRound);
            Assert.AreEqual(chunks1[0].IsLast, false);
            Assert.AreEqual(chunks1[1].IsLast, true);

            Assert.AreEqual(chunks2.Count, 1);
            Assert.IsNull(chunks2[0].Data.Message);
            Assert.AreEqual(chunks2[0].Data.ControlMessage, MapControlMessage.Stop);
            Assert.AreEqual(chunks2[0].IsLast, true);

            var fullMessage1 = dataConverter.FullMessage(chunks1);
            var fullMessage2 = dataConverter.FullMessage(chunks2);

            Assert.AreEqual(fullMessage1.Message[0], baseMessage[0]);
            Assert.AreEqual(fullMessage1.Message[1], baseMessage[1]);
            Assert.AreEqual(fullMessage1.Message[2], baseMessage[2]);
            Assert.AreEqual(fullMessage1.ControlMessage, chunks1[0].Data.ControlMessage);
            Assert.IsNull(fullMessage2.Message);
            Assert.AreEqual(fullMessage2.ControlMessage, chunks2[0].Data.ControlMessage);
        }

        [NamedParameter("Chunk size.")]
        private sealed class ChunkSize : Name<int>
        {
        }

        private class PipelineIntDataConverter : IPipelineDataConverter<int[]>
        {
            private readonly int _chunkSize;

            [Inject]
            private PipelineIntDataConverter([Parameter(typeof(ChunkSize))] int chunkSize)
            {
                _chunkSize = chunkSize;
            }

            public List<PipelineMessage<int[]>> PipelineMessage(int[] message)
            {
                var messageList = new List<PipelineMessage<int[]>>();
                var totalChunks = message.Length / _chunkSize;

                if (message.Length % _chunkSize != 0)
                {
                    totalChunks++;
                }

                var counter = 0;
                for (var i = 0; i < message.Length; i += _chunkSize)
                {
                    var data = new int[Math.Min(_chunkSize, message.Length - i)];
                    Buffer.BlockCopy(message, i * sizeof(int), data, 0, data.Length * sizeof(int));

                    messageList.Add(counter == totalChunks - 1
                        ? new PipelineMessage<int[]>(data, true)
                        : new PipelineMessage<int[]>(data, false));

                    counter++;
                }

                return messageList;
            }

            public int[] FullMessage(List<PipelineMessage<int[]>> pipelineMessage)
            {
                var size = pipelineMessage.Select(x => x.Data.Length).Sum();
                var data = new int[size];
                var offset = 0;

                foreach (var message in pipelineMessage)
                {
                    Buffer.BlockCopy(message.Data, 0, data, offset, message.Data.Length * sizeof(int));
                    offset += message.Data.Length * sizeof(int);
                }

                return data;
            }
        }
    }
}
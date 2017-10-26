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
using System.Collections.Concurrent;
using System.Net;
using System.Reactive;
using Org.Apache.REEF.Wake.Avro;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Tang.Implementations.Tang;
using org.apache.reef.wake.tests.message;
using Xunit;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Wake.Tests
{
    /// <summary>
    /// Observer to receive and verify test message contents.
    /// </summary>
    internal sealed class TestMessageObserver : IObserver<IMessageInstance<AvroTestMessage>>
    {
        private readonly IMessageInstance<AvroTestMessage> _messageInstance;

        public TestMessageObserver(long seq, AvroTestMessage msg)
        {
            _messageInstance = new MessageInstance<AvroTestMessage>(seq, msg);
        }

        public void OnNext(IMessageInstance<AvroTestMessage> otherMessageInstance)
        {
            Assert.Equal(_messageInstance.Message.number, otherMessageInstance.Message.number);
            Assert.Equal(_messageInstance.Message.data, otherMessageInstance.Message.data);
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException("This method should never be called");
        }

        public void OnCompleted()
        {
            throw new NotImplementedException("This method should never be called");
        }
    }

    public sealed class TestProtocolSerializer
    {
        private static readonly ITang Tang = TangFactory.GetTang();
        private static readonly IPAddress ListeningAddress = IPAddress.Parse("127.0.0.1");
        private static readonly ByteCodec Codec = new ByteCodec();

        /// <summary>
        /// Setup two way communication between two remote managers through the loopback
        /// network and verify that Avro messages are properly serialized and deserialzied
        /// by the ProtocolSerializer class.
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        public void TestTwoWayCommunication()
        {
            // Test data.
            int[] numbers = { 12, 25 };
            string[] strings = { "The first string", "The second string" };

            BlockingCollection<byte[]> queue1 = new BlockingCollection<byte[]>();
            BlockingCollection<byte[]> queue2 = new BlockingCollection<byte[]>();

            IConfiguration config = Tang.NewConfigurationBuilder()
                .BindStringNamedParam<ProtocolSerializer.AssemblyName>(this.GetType().Assembly.FullName)
                .BindStringNamedParam<ProtocolSerializer.MessageNamespace>("org.apache.reef.wake.tests.message")
                .Build();

            var injector = Tang.NewInjector(config);
            var remoteManagerFactory = injector.GetInstance<IRemoteManagerFactory>();
            var serializer = injector.GetInstance<ProtocolSerializer>();

            using (var remoteManager1 = remoteManagerFactory.GetInstance(ListeningAddress, Codec))
            using (var remoteManager2 = remoteManagerFactory.GetInstance(ListeningAddress, Codec))
            {
                // Register observers for remote manager 1 and remote manager 2
                var remoteEndpoint = new IPEndPoint(ListeningAddress, 0);
                remoteManager1.RegisterObserver(remoteEndpoint, Observer.Create<byte[]>(queue1.Add));
                remoteManager2.RegisterObserver(remoteEndpoint, Observer.Create<byte[]>(queue2.Add));

                var msg1 = new AvroTestMessage(numbers[0], strings[0]);
                var msg2 = new AvroTestMessage(numbers[1], strings[1]);

                // Remote manager 1 sends avro message to remote manager 2
                var remoteObserver1 = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                remoteObserver1.OnNext(serializer.Write(msg1, 1));

                // Remote manager 2 sends avro message to remote manager 1
                var remoteObserver2 = remoteManager2.GetRemoteObserver(remoteManager1.LocalEndpoint);
                remoteObserver2.OnNext(serializer.Write(msg2, 2));

                // Verify the messages are properly received.
                serializer.Read(queue1.Take(), new TestMessageObserver(2, msg2));
                serializer.Read(queue2.Take(), new TestMessageObserver(1, msg1));
            }
        }
    }
}

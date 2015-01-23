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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reactive;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Reef.Wake.Remote;
using Org.Apache.Reef.Wake.Remote.Impl;
using Org.Apache.Reef.Wake.Util;

namespace Org.Apache.Reef.Wake.Test
{
    [TestClass]
    public class TransportTest
    {
        [TestMethod]
        public void TestTransportServer()
        {
            ICodec<string> codec = new StringCodec();
            int port = NetworkUtils.GenerateRandomPort(6000, 7000);

            BlockingCollection<string> queue = new BlockingCollection<string>();
            List<string> events = new List<string>();

            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, port);
            var remoteHandler = Observer.Create<TransportEvent<string>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new TransportServer<string>(endpoint, remoteHandler, codec))
            {
                server.Run();

                IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), port);
                using (var client = new TransportClient<string>(remoteEndpoint, codec))
                {
                    client.Send("Hello");
                    client.Send(", ");
                    client.Send("World!");

                    events.Add(queue.Take());
                    events.Add(queue.Take());
                    events.Add(queue.Take());
                } 
            }

            Assert.AreEqual(3, events.Count);
        }

        [TestMethod]
        public void TestTransportServerEvent()
        {
            ICodec<TestEvent> codec = new TestEventCodec();
            int port = NetworkUtils.GenerateRandomPort(6000, 7000);

            BlockingCollection<TestEvent> queue = new BlockingCollection<TestEvent>();
            List<TestEvent> events = new List<TestEvent>();

            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, port);
            var remoteHandler = Observer.Create<TransportEvent<TestEvent>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new TransportServer<TestEvent>(endpoint, remoteHandler, codec))
            {
                server.Run();

                IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), port);
                using (var client = new TransportClient<TestEvent>(remoteEndpoint, codec))
                {
                    client.Send(new TestEvent("Hello"));
                    client.Send(new TestEvent(", "));
                    client.Send(new TestEvent("World!"));

                    events.Add(queue.Take());
                    events.Add(queue.Take());
                    events.Add(queue.Take());
                } 
            }

            Assert.AreEqual(3, events.Count);
        }

        [TestMethod]
        public void TestTransportSenderStage()
        {
            ICodec<string> codec = new StringCodec();
            int port = NetworkUtils.GenerateRandomPort(6000, 7000);
            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, port);

            List<string> events = new List<string>();
            BlockingCollection<string> queue = new BlockingCollection<string>();

            // Server echoes the message back to the client
            var remoteHandler = Observer.Create<TransportEvent<string>>(tEvent => tEvent.Link.Write(tEvent.Data));

            using (TransportServer<string> server = new TransportServer<string>(endpoint, remoteHandler, codec))
            {
                server.Run();

                var clientHandler = Observer.Create<TransportEvent<string>>(tEvent => queue.Add(tEvent.Data));
                IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), port);
                using (var client = new TransportClient<string>(remoteEndpoint, codec, clientHandler))
                {
                    client.Send("Hello");
                    client.Send(", ");
                    client.Send(" World");

                    events.Add(queue.Take());
                    events.Add(queue.Take());
                    events.Add(queue.Take());
                } 
            }

            Assert.AreEqual(3, events.Count);
        }

        [TestMethod]
        public void TestRaceCondition()
        {
            ICodec<string> codec = new StringCodec();
            int port = NetworkUtils.GenerateRandomPort(6000, 7000);

            BlockingCollection<string> queue = new BlockingCollection<string>();
            List<string> events = new List<string>();
            int numEventsExpected = 150;

            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, port);
            var remoteHandler = Observer.Create<TransportEvent<string>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new TransportServer<string>(endpoint, remoteHandler, codec))
            {
                server.Run();

                for (int i = 0; i < numEventsExpected / 3; i++)
                {
                    Task.Run(() =>
                    {
                        IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), port);
                        using (var client = new TransportClient<string>(remoteEndpoint, codec))
                        {
                            client.Send("Hello");
                            client.Send(", ");
                            client.Send("World!");
                        }
                    });
                }

                for (int i = 0; i < numEventsExpected; i++)
                {
                    events.Add(queue.Take());
                }
            }

            Assert.AreEqual(numEventsExpected, events.Count);
        }

        private class TestEvent
        {
            public TestEvent(string message)
            {
                Message = message;
            }

            public string Message { get; set; }

            public override string ToString()
            {
                return "TestEvent: " + Message;
            }
        }

        private class TestEventCodec : ICodec<TestEvent>
        {
            public byte[] Encode(TestEvent obj)
            {
                return new StringCodec().Encode(obj.Message);
            }

            public TestEvent Decode(byte[] data)
            {
                return new TestEvent(new StringCodec().Decode(data));
            }
        }
    }
}

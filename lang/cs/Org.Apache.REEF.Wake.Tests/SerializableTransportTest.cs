﻿/**
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
using System.IO;
using System.Net;
using System.Reactive;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Wake.Tests
{
    [TestClass]
    public class SerializableTransportTest
    {
        [TestMethod]
        public void TestSerializableTransportServer()
        {
            int port = NetworkUtils.GenerateRandomPort(6000, 7000);

            BlockingCollection<WritableString> queue = new BlockingCollection<WritableString>();
            List<string> events = new List<string>();

            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, port);
            var remoteHandler = Observer.Create<TransportEvent<WritableString>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new SerializableTransportServer<WritableString>(endpoint, remoteHandler))
            {
                server.Run();

                IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), port);
                using (var client = new SerializableTransportClient<WritableString>(remoteEndpoint))
                {
                    client.Send(new WritableString("Hello"));
                    client.Send(new WritableString(", "));
                    client.Send(new WritableString("World!"));

                    events.Add(queue.Take().Data);
                    events.Add(queue.Take().Data);
                    events.Add(queue.Take().Data);
                } 
            }

            Assert.AreEqual(3, events.Count);
        }

        [TestMethod]
        public void TestSerializableTransportServerEvent()
        {
            int port = NetworkUtils.GenerateRandomPort(6000, 7000);

            BlockingCollection<TestEvent> queue = new BlockingCollection<TestEvent>();
            List<TestEvent> events = new List<TestEvent>();

            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, port);
            var remoteHandler = Observer.Create<TransportEvent<TestEvent>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new SerializableTransportServer<TestEvent>(endpoint, remoteHandler))
            {
                server.Run();

                IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), port);
                using (var client = new SerializableTransportClient<TestEvent>(remoteEndpoint))
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
        public void TestSerializableTransportSenderStage()
        {
            int port = NetworkUtils.GenerateRandomPort(6000, 7000);
            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, port);

            List<string> events = new List<string>();
            BlockingCollection<WritableString> queue = new BlockingCollection<WritableString>();

            // Server echoes the message back to the client
            var remoteHandler = Observer.Create<TransportEvent<WritableString>>(tEvent => tEvent.Link.Write(tEvent.Data));

            using (SerializableTransportServer<WritableString> server = new SerializableTransportServer<WritableString>(endpoint, remoteHandler))
            {
                server.Run();

                var clientHandler = Observer.Create<TransportEvent<WritableString>>(tEvent => queue.Add(tEvent.Data));
                IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), port);
                using (var client = new SerializableTransportClient<WritableString>(remoteEndpoint, clientHandler))
                {
                    client.Send(new WritableString("Hello"));
                    client.Send(new WritableString(", "));
                    client.Send(new WritableString(" World"));

                    events.Add(queue.Take().Data);
                    events.Add(queue.Take().Data);
                    events.Add(queue.Take().Data);
                } 
            }

            Assert.AreEqual(3, events.Count);
        }

        [TestMethod]
        public void TestSerializableRaceCondition()
        {
            int port = NetworkUtils.GenerateRandomPort(6000, 7000);

            BlockingCollection<WritableString> queue = new BlockingCollection<WritableString>();
            List<string> events = new List<string>();
            int numEventsExpected = 150;

            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, port);
            var remoteHandler = Observer.Create<TransportEvent<WritableString>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new SerializableTransportServer<WritableString>(endpoint, remoteHandler))
            {
                server.Run();

                for (int i = 0; i < numEventsExpected / 3; i++)
                {
                    Task.Run(() =>
                    {
                        IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), port);
                        using (var client = new SerializableTransportClient<WritableString>(remoteEndpoint))
                        {
                            client.Send(new WritableString("Hello"));
                            client.Send(new WritableString(", "));
                            client.Send(new WritableString("World!"));
                        }
                    });
                }

                for (int i = 0; i < numEventsExpected; i++)
                {
                    events.Add(queue.Take().Data);
                }
            }

            Assert.AreEqual(numEventsExpected, events.Count);
        }
    }
}

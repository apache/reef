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
using System.IO;
using System.Net;
using System.Reactive;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Wake.Tests
{
    /// <summary>
    /// Tests the WritableTransportServer, WritableTransportClient and WritableLink.
    /// Basically the Wake transport layer.
    /// </summary>
    [TestClass]
    [Obsolete("Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295 ", false)]
    public class WritableTransportTest
    {
        private readonly ITcpPortProvider _tcpPortProvider = GetTcpProvider(6000,7000);

        /// <summary>
        /// Tests whether WritableTransportServer receives 
        /// string messages from WritableTransportClient
        /// </summary>
        [TestMethod]
        public void TestWritableTransportServer()
        {
            BlockingCollection<WritableString> queue = new BlockingCollection<WritableString>();
            List<string> events = new List<string>();

            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, 0);
            var remoteHandler = Observer.Create<TransportEvent<WritableString>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new WritableTransportServer<WritableString>(endpoint, remoteHandler, _tcpPortProvider))
            {
                server.Run();

                IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), server.LocalEndpoint.Port);
                using (var client = new WritableTransportClient<WritableString>(remoteEndpoint))
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
            Assert.AreEqual(events[0], "Hello");
            Assert.AreEqual(events[1], ", ");
            Assert.AreEqual(events[2], "World!");
        }

       
        /// <summary>
        /// Checks whether WritableTransportClient is able to receive messages from remote host
        /// </summary>
        [TestMethod]
        public void TestWritableTransportSenderStage()
        {
            
            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, 0);

            List<string> events = new List<string>();
            BlockingCollection<WritableString> queue = new BlockingCollection<WritableString>();

            // Server echoes the message back to the client
            var remoteHandler = Observer.Create<TransportEvent<WritableString>>(tEvent => tEvent.Link.Write(tEvent.Data));

            using (var server = new WritableTransportServer<WritableString>(endpoint, remoteHandler, _tcpPortProvider))
            {
                server.Run();

                var clientHandler = Observer.Create<TransportEvent<WritableString>>(tEvent => queue.Add(tEvent.Data));
                IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), server.LocalEndpoint.Port);
                using (var client = new WritableTransportClient<WritableString>(remoteEndpoint, clientHandler))
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
            Assert.AreEqual(events[0], "Hello");
            Assert.AreEqual(events[1], ", ");
            Assert.AreEqual(events[2], " World");
        }

        /// <summary>
        /// Checks whether WritableTransportClient and WritableTransportServer works 
        /// in asynchronous condition while sending messages asynchronously from different 
        /// threads
        /// </summary>
        [TestMethod]
        public void TestWritableRaceCondition()
        {
            BlockingCollection<WritableString> queue = new BlockingCollection<WritableString>();
            List<string> events = new List<string>();
            int numEventsExpected = 150;

            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, 0);
            var remoteHandler = Observer.Create<TransportEvent<WritableString>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new WritableTransportServer<WritableString>(endpoint, remoteHandler, _tcpPortProvider))
            {
                server.Run();

                for (int i = 0; i < numEventsExpected / 3; i++)
                {
                    Task.Run(() =>
                    {
                        IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), server.LocalEndpoint.Port);
                        using (var client = new WritableTransportClient<WritableString>(remoteEndpoint))
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

        private static ITcpPortProvider GetTcpProvider(int portRangeStart, int portRangeEnd)
        {
            var configuration = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation<ITcpPortProvider, TcpPortProvider>()
                .BindIntNamedParam<TcpPortRangeStart>(portRangeStart.ToString())
                .BindIntNamedParam<TcpPortRangeCount>((portRangeEnd - portRangeStart + 1).ToString())
                .Build();
            return TangFactory.GetTang().NewInjector(configuration).GetInstance<ITcpPortProvider>();
        }
    }
}

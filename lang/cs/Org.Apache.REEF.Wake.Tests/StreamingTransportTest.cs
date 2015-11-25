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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Reactive;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Org.Apache.REEF.Wake.StreamingCodec;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;
using Xunit;

namespace Org.Apache.REEF.Wake.Tests
{
    /// <summary>
    /// Tests the StreamingTransportServer, StreamingTransportClient and StreamingLink.
    /// Basically the Wake transport layer.
    /// </summary>
    public class StreamingTransportTest
    {
        private readonly ITcpPortProvider _tcpPortProvider = GetTcpProvider(9900, 9940);
        private readonly IInjector _injector = TangFactory.GetTang().NewInjector();

        /// <summary>
        /// Tests whether StreamingTransportServer receives 
        /// string messages from StreamingTransportClient
        /// </summary>
        [Fact]
        public void TestStreamingTransportServer()
        {
            BlockingCollection<string> queue = new BlockingCollection<string>();
            List<string> events = new List<string>();
            IStreamingCodec<string> stringCodec = _injector.GetInstance<StringStreamingCodec>();

            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, 0);
            var remoteHandler = Observer.Create<TransportEvent<string>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new StreamingTransportServer<string>(endpoint.Address, remoteHandler, _tcpPortProvider, stringCodec))
            {
                server.Run();

                IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), server.LocalEndpoint.Port);
                using (var client = new StreamingTransportClient<string>(remoteEndpoint, stringCodec))
                {
                    client.Send("Hello");
                    client.Send(", ");
                    client.Send("World!");

                    events.Add(queue.Take());
                    events.Add(queue.Take());
                    events.Add(queue.Take());
                } 
            }

            Assert.Equal(3, events.Count);
            Assert.Equal(events[0], "Hello");
            Assert.Equal(events[1], ", ");
            Assert.Equal(events[2], "World!");
        }

        /// <summary>
        /// Checks whether StreamingTransportClient is able to receive messages from remote host
        /// </summary>
        [Fact]
        public void TestStreamingTransportSenderStage()
        {
            
            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, 0);

            List<string> events = new List<string>();
            BlockingCollection<string> queue = new BlockingCollection<string>();
            IStreamingCodec<string> stringCodec = _injector.GetInstance<StringStreamingCodec>();

            // Server echoes the message back to the client
            var remoteHandler = Observer.Create<TransportEvent<string>>(tEvent => tEvent.Link.Write(tEvent.Data));

            using (var server = new StreamingTransportServer<string>(endpoint.Address, remoteHandler, _tcpPortProvider, stringCodec))
            {
                server.Run();

                var clientHandler = Observer.Create<TransportEvent<string>>(tEvent => queue.Add(tEvent.Data));
                IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), server.LocalEndpoint.Port);
                using (var client = new StreamingTransportClient<string>(remoteEndpoint, clientHandler, stringCodec))
                {
                    client.Send("Hello");
                    client.Send(", ");
                    client.Send(" World");

                    events.Add(queue.Take());
                    events.Add(queue.Take());
                    events.Add(queue.Take());
                } 
            }

            Assert.Equal(3, events.Count);
            Assert.Equal(events[0], "Hello");
            Assert.Equal(events[1], ", ");
            Assert.Equal(events[2], " World");
        }

        /// <summary>
        /// Checks whether StreamingTransportClient and StreamingTransportServer works 
        /// in asynchronous condition while sending messages asynchronously from different 
        /// threads
        /// </summary>
        [Fact]
        public void TestStreamingRaceCondition()
        {
            BlockingCollection<string> queue = new BlockingCollection<string>();
            List<string> events = new List<string>();
            IStreamingCodec<string> stringCodec = _injector.GetInstance<StringStreamingCodec>();
            int numEventsExpected = 150;

            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, 0);
            var remoteHandler = Observer.Create<TransportEvent<string>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new StreamingTransportServer<string>(endpoint.Address, remoteHandler, _tcpPortProvider, stringCodec))
            {
                server.Run();

                for (int i = 0; i < numEventsExpected / 3; i++)
                {
                    Task.Run(() =>
                    {
                        IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), server.LocalEndpoint.Port);
                        using (var client = new StreamingTransportClient<string>(remoteEndpoint, stringCodec))
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

            Assert.Equal(numEventsExpected, events.Count);

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

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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Reactive;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Xunit;

namespace Org.Apache.REEF.Wake.Tests
{
    public class TransportTest
    {
        private readonly IPAddress _localIpAddress = IPAddress.Parse("127.0.0.1");
        private readonly ITcpPortProvider _tcpPortProvider = GetTcpProvider(8900, 8940);
        private readonly ITcpClientConnectionFactory _tcpClientFactory = GetTcpClientFactory(5, 500);

        [Fact]
        public void TestTransportServer()
        {
            ICodec<string> codec = new StringCodec();

            BlockingCollection<string> queue = new BlockingCollection<string>();
            List<string> events = new List<string>();

            IPEndPoint endpoint = new IPEndPoint(_localIpAddress, 0);
            var remoteHandler = Observer.Create<TransportEvent<string>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new TransportServer<string>(endpoint, remoteHandler, codec, _tcpPortProvider))
            {
                server.Run();

                IPEndPoint remoteEndpoint = new IPEndPoint(_localIpAddress, server.LocalEndpoint.Port);
                using (var client = new TransportClient<string>(remoteEndpoint, codec, _tcpClientFactory))
                {
                    client.Send("TestTransportServer - 1");
                    client.Send("TestTransportServer - 2");
                    client.Send("TestTransportServer - 3");

                    events.Add(queue.Take());
                    events.Add(queue.Take());
                    events.Add(queue.Take());
                } 
            }

            Assert.Equal(3, events.Count);
            Assert.Equal("TestTransportServer - 1", events[0]);
            Assert.Equal("TestTransportServer - 2", events[1]);
            Assert.Equal("TestTransportServer - 3", events[2]);
        }

        [Fact]
        public void TestTransportServerEvent()
        {
            ICodec<TestEvent> codec = new TestEventCodec();

            BlockingCollection<TestEvent> queue = new BlockingCollection<TestEvent>();
            List<TestEvent> events = new List<TestEvent>();

            IPEndPoint endpoint = new IPEndPoint(_localIpAddress, 0);
            var remoteHandler = Observer.Create<TransportEvent<TestEvent>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new TransportServer<TestEvent>(endpoint, remoteHandler, codec, _tcpPortProvider))
            {
                server.Run();

                IPEndPoint remoteEndpoint = new IPEndPoint(_localIpAddress, server.LocalEndpoint.Port);
                using (var client = new TransportClient<TestEvent>(remoteEndpoint, codec, _tcpClientFactory))
                {
                    client.Send(new TestEvent("TestTransportServerEvent - 1"));
                    client.Send(new TestEvent("TestTransportServerEvent - 2"));
                    client.Send(new TestEvent("TestTransportServerEvent - 3"));

                    events.Add(queue.Take());
                    events.Add(queue.Take());
                    events.Add(queue.Take());
                } 
            }

            Assert.Equal(3, events.Count);
            Assert.Equal("TestTransportServerEvent - 1", events[0].Message);
            Assert.Equal("TestTransportServerEvent - 2", events[1].Message);
            Assert.Equal("TestTransportServerEvent - 3", events[2].Message);
        }

        [Fact]
        public void TestTransportSenderStage()
        {
            ICodec<string> codec = new StringCodec();
            IPEndPoint endpoint = new IPEndPoint(_localIpAddress, 0);

            List<string> events = new List<string>();
            BlockingCollection<string> queue = new BlockingCollection<string>();

            // Server echoes the message back to the client
            var remoteHandler = Observer.Create<TransportEvent<string>>(tEvent => tEvent.Link.Write(tEvent.Data));

            using (TransportServer<string> server = new TransportServer<string>(endpoint, remoteHandler, codec, _tcpPortProvider))
            {
                server.Run();

                var clientHandler = Observer.Create<TransportEvent<string>>(tEvent => queue.Add(tEvent.Data));
                IPEndPoint remoteEndpoint = new IPEndPoint(_localIpAddress, server.LocalEndpoint.Port);
                using (var client = new TransportClient<string>(remoteEndpoint, codec, clientHandler, _tcpClientFactory))
                {
                    client.Send("TestTransportSenderStage - 1");
                    client.Send("TestTransportSenderStage - 2");
                    client.Send("TestTransportSenderStage - 3");

                    events.Add(queue.Take());
                    events.Add(queue.Take());
                    events.Add(queue.Take());
                } 
            }

            Assert.Equal(3, events.Count);
            Assert.Equal("TestTransportSenderStage - 1", events[0]);
            Assert.Equal("TestTransportSenderStage - 2", events[1]);
            Assert.Equal("TestTransportSenderStage - 3", events[2]);
        }

        [Fact]
        public void TestRaceCondition()
        {
            ICodec<string> codec = new StringCodec();
            var port = 0;
            BlockingCollection<string> queue = new BlockingCollection<string>();
            List<string> events = new List<string>();
            int numEventsExpected = 150;

            IPEndPoint endpoint = new IPEndPoint(IPAddress.Any, port);
            var remoteHandler = Observer.Create<TransportEvent<string>>(tEvent => queue.Add(tEvent.Data));

            using (var server = new TransportServer<string>(endpoint, remoteHandler, codec, _tcpPortProvider))
            {
                server.Run();

                int counter = 0;
                for (int i = 0; i < numEventsExpected / 3; i++)
                {
                    Task.Run(() =>
                    {
                        IPEndPoint remoteEndpoint = new IPEndPoint(_localIpAddress, server.LocalEndpoint.Port);
                        using (var client = new TransportClient<string>(remoteEndpoint, codec, _tcpClientFactory))
                        {
                            for (int j = 0; j < 3; j++)
                            {
                                client.Send("TestRaceCondition - " + counter++);
                            }
                        }
                    });
                }

                for (int i = 0; i < numEventsExpected; i++)
                {
                    string e;
                    do
                    {
                        e = queue.Take();
                    }
                    while (e == null);

                    events.Add(e);
                }
            }

            Assert.Equal(numEventsExpected, events.Count);
            foreach (var e in events)
            {
                Assert.True(e.StartsWith("TestRaceCondition - "), "Unexpected event [" + e + "]");
            }
        }

        private class TestEvent
        {
            public TestEvent(string message)
            {
                Message = message;
            }

            public string Message { get; private set; }

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

        private static ITcpPortProvider GetTcpProvider(int portRangeStart, int portRangeEnd)
        {
            var configuration = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation<ITcpPortProvider, TcpPortProvider>()
                .BindIntNamedParam<TcpPortRangeStart>(portRangeStart.ToString())
                .BindIntNamedParam<TcpPortRangeCount>((portRangeEnd - portRangeStart + 1).ToString())
                .Build();
            return TangFactory.GetTang().NewInjector(configuration).GetInstance<ITcpPortProvider>();
        }

        private static ITcpClientConnectionFactory GetTcpClientFactory(int connectionRetryCount, int sleepTimeInMs)
        {
            var config =
                TangFactory.GetTang()
                    .NewConfigurationBuilder()
                    .BindIntNamedParam<ConnectionRetryCount>(connectionRetryCount.ToString())
                    .BindIntNamedParam<SleepTimeInMs>(sleepTimeInMs.ToString())
                    .Build();
            return TangFactory.GetTang().NewInjector(config).GetInstance<ITcpClientConnectionFactory>();
        }
    }
}

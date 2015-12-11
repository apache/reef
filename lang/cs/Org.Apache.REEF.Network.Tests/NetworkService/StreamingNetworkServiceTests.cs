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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Network.NetworkService.Codec;
using Org.Apache.REEF.Network.Tests.NamingService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.StreamingCodec;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;
using Org.Apache.REEF.Wake.Util;
using Xunit;

namespace Org.Apache.REEF.Network.Tests.NetworkService
{
    /// <summary>
    /// Tests for Streaming Network Service
    /// </summary>
    public class StreamingNetworkServiceTests
    {
        /// <summary>
        /// Tests one way communication between two network services
        /// </summary>
        [Fact]
        public void TestStreamingNetworkServiceOneWayCommunication()
        {
            int networkServicePort1 = NetworkUtils.GenerateRandomPort(6000, 7000);
            int networkServicePort2 = NetworkUtils.GenerateRandomPort(7001, 8000);

            BlockingCollection<string> queue;

            using (var nameServer = NameServerTests.BuildNameServer())
            {
                IPEndPoint endpoint = nameServer.LocalEndpoint;
                int nameServerPort = endpoint.Port;
                string nameServerAddr = endpoint.Address.ToString();

                var handlerConf1 =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder()
                        .BindImplementation(GenericType<IObserver<NsMessage<string>>>.Class,
                            GenericType<NetworkMessageHandler>.Class)
                        .Build();

                var handlerConf2 =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder()
                        .BindImplementation(GenericType<IObserver<NsMessage<string>>>.Class,
                            GenericType<MessageHandler>.Class)
                        .Build();

                var networkServiceInjection1 = BuildNetworkService(networkServicePort1, nameServerPort, nameServerAddr,
                    handlerConf1);

                var networkServiceInjection2 = BuildNetworkService(networkServicePort2, nameServerPort, nameServerAddr,
                   handlerConf2);

                using (INetworkService<string> networkService1 = networkServiceInjection1.GetInstance<StreamingNetworkService<string>>())
                using (INetworkService<string> networkService2 = networkServiceInjection2.GetInstance<StreamingNetworkService<string>>())
                {
                    queue = networkServiceInjection2.GetInstance<MessageHandler>().Queue;
                    IIdentifier id1 = new StringIdentifier("service1");
                    IIdentifier id2 = new StringIdentifier("service2");
                    networkService1.Register(id1);
                    networkService2.Register(id2);

                    using (IConnection<string> connection = networkService1.NewConnection(id2))
                    {
                        connection.Open();
                        connection.Write("abc");
                        connection.Write("def");
                        connection.Write("ghi");

                        Assert.Equal("abc", queue.Take());
                        Assert.Equal("def", queue.Take());
                        Assert.Equal("ghi", queue.Take());
                    }
                }
            }
        }

        /// <summary>
        /// Tests two way communication between two network services
        /// </summary>
        [Fact]
        public void TestStreamingNetworkServiceTwoWayCommunication()
        {
            int networkServicePort1 = NetworkUtils.GenerateRandomPort(6000, 7000);
            int networkServicePort2 = NetworkUtils.GenerateRandomPort(7001, 8000);

            BlockingCollection<string> queue1;
            BlockingCollection<string> queue2;

            using (var nameServer = NameServerTests.BuildNameServer())
            {
                IPEndPoint endpoint = nameServer.LocalEndpoint;
                int nameServerPort = endpoint.Port;
                string nameServerAddr = endpoint.Address.ToString();

                var handlerConf =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder()
                        .BindImplementation(GenericType<IObserver<NsMessage<string>>>.Class,
                            GenericType<MessageHandler>.Class)
                        .Build();

                var networkServiceInjection1 = BuildNetworkService(networkServicePort1, nameServerPort, nameServerAddr,
                    handlerConf);

                var networkServiceInjection2 = BuildNetworkService(networkServicePort2, nameServerPort, nameServerAddr,
                   handlerConf);

                using (INetworkService<string> networkService1 = networkServiceInjection1.GetInstance<StreamingNetworkService<string>>())
                using (INetworkService<string> networkService2 = networkServiceInjection2.GetInstance<StreamingNetworkService<string>>())
                {
                    queue1 = networkServiceInjection1.GetInstance<MessageHandler>().Queue;
                    queue2 = networkServiceInjection2.GetInstance<MessageHandler>().Queue;

                    IIdentifier id1 = new StringIdentifier("service1");
                    IIdentifier id2 = new StringIdentifier("service2");
                    networkService1.Register(id1);
                    networkService2.Register(id2);

                    using (IConnection<string> connection1 = networkService1.NewConnection(id2))
                    using (IConnection<string> connection2 = networkService2.NewConnection(id1))
                    {
                        connection1.Open();
                        connection1.Write("abc");
                        connection1.Write("def");
                        connection1.Write("ghi");

                        connection2.Open();
                        connection2.Write("jkl");
                        connection2.Write("nop");

                        Assert.Equal("abc", queue2.Take());
                        Assert.Equal("def", queue2.Take());
                        Assert.Equal("ghi", queue2.Take());

                        Assert.Equal("jkl", queue1.Take());
                        Assert.Equal("nop", queue1.Take());
                    }
                }
            }
        }

        /// <summary>
        /// Tests StreamingCodecFunctionCache
        /// </summary>
        [Fact]
        public void TestStreamingCodecFunctionCache()
        {
            IConfiguration conf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IStreamingCodec<B>>.Class, GenericType<BStreamingCodec>.Class)
                .Build();
            IInjector injector = TangFactory.GetTang().NewInjector(conf);
            
            StreamingCodecFunctionCache<A> cache = new StreamingCodecFunctionCache<A>(injector);

            var readFunc = cache.ReadFunction(typeof(B));
            var writeFunc = cache.WriteFunction(typeof(B));
            var readAsyncFunc = cache.ReadAsyncFunction(typeof(B));
            var writeAsyncFunc = cache.WriteAsyncFunction(typeof(B));

            var stream = new MemoryStream();
            IDataWriter writer = new StreamDataWriter(stream);
            IDataReader reader = new StreamDataReader(stream);

            B val = new B();
            val.Value1 = "hello";
            val.Value2 = "reef";

            writeFunc(val, writer);

            val.Value1 = "helloasync";
            val.Value2 = "reefasync";
            CancellationToken token = new CancellationToken();
            
            var asyncResult = writeAsyncFunc.BeginInvoke(val, writer, token, null, null);
            writeAsyncFunc.EndInvoke(asyncResult);
            
            stream.Position = 0;
            A res = readFunc(reader);
            B resB1 = res as B;

            asyncResult = readAsyncFunc.BeginInvoke(reader, token, null, null);
            res = readAsyncFunc.EndInvoke(asyncResult);
            B resB2 = res as B;
            
            Assert.Equal("hello", resB1.Value1);
            Assert.Equal("reef", resB1.Value2);
            Assert.Equal("helloasync", resB2.Value1);
            Assert.Equal("reefasync", resB2.Value2);
        }

        /// <summary>
        /// Creates an instance of network service.
        /// </summary>
        /// <param name="networkServicePort">The port that the NetworkService will listen on</param>
        /// <param name="nameServicePort">The port of the NameServer</param>
        /// <param name="nameServiceAddr">The ip address of the NameServer</param>
        /// <param name="handlerConf">The configuration of observer to handle incoming messages</param>
        /// <returns></returns>
        private IInjector BuildNetworkService(
            int networkServicePort,
            int nameServicePort,
            string nameServiceAddr,
            IConfiguration handlerConf)
        {
            var networkServiceConf = TangFactory.GetTang().NewConfigurationBuilder(handlerConf)
                .BindNamedParameter<NetworkServiceOptions.NetworkServicePort, int>(
                    GenericType<NetworkServiceOptions.NetworkServicePort>.Class,
                    networkServicePort.ToString(CultureInfo.CurrentCulture))
                .BindNamedParameter<NamingConfigurationOptions.NameServerPort, int>(
                    GenericType<NamingConfigurationOptions.NameServerPort>.Class,
                    nameServicePort.ToString(CultureInfo.CurrentCulture))
                .BindNamedParameter<NamingConfigurationOptions.NameServerAddress, string>(
                    GenericType<NamingConfigurationOptions.NameServerAddress>.Class,
                    nameServiceAddr)
                .BindImplementation(GenericType<INameClient>.Class, GenericType<NameClient>.Class)
                .BindImplementation(GenericType<IStreamingCodec<string>>.Class, GenericType<StringStreamingCodec>.Class)
                .Build();

            return TangFactory.GetTang().NewInjector(networkServiceConf);
        }

        public class A
        {
            public string Value1;
        }

        public class B : A
        {
            public string Value2;
        }

        public class BStreamingCodec : IStreamingCodec<B>
        {
            [Inject]
            public BStreamingCodec()
            {
            }

            public B Read(IDataReader reader)
            {
                B val = new B();
                val.Value1 = reader.ReadString();
                val.Value2 = reader.ReadString();
                return val;
            }

            public void Write(B obj, IDataWriter writer)
            {
                writer.WriteString(obj.Value1);
                writer.WriteString(obj.Value2);
            }

            public async Task<B> ReadAsync(IDataReader reader, CancellationToken token)
            {
                B val = new B();
                val.Value1 = await reader.ReadStringAsync(token);
                val.Value2 = await reader.ReadStringAsync(token);
                return val;
            }

            public async Task WriteAsync(B obj, IDataWriter writer, CancellationToken token)
            {
                await writer.WriteStringAsync(obj.Value1, token);
                await writer.WriteStringAsync(obj.Value2, token);
            }
        } 

        /// <summary>
        /// The observer to handle incoming messages for string
        /// </summary>
        private class MessageHandler : IObserver<NsMessage<string>>
        {
            private readonly BlockingCollection<string> _queue;

            public BlockingCollection<string> Queue
            {
                get { return _queue; }
            } 

            [Inject]
            private MessageHandler()
            {
                _queue = new BlockingCollection<string>();
            }

            public void OnNext(NsMessage<string> value)
            {
                _queue.Add(value.Data.First());
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// The network handler to handle incoming Streaming NsMessages
        /// </summary>
        private class NetworkMessageHandler : IObserver<NsMessage<string>>
        {
            [Inject]
            public NetworkMessageHandler()
            {
            }

            public void OnNext(NsMessage<string> value)
            {
            }

            public void OnError(Exception error)
            {
            }

            public void OnCompleted()
            {
            }
        }
    }
}

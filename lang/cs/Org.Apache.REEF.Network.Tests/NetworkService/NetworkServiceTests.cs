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
using System.Linq;
using System.Net;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Network.Tests.NamingService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.Util;
using Xunit;

namespace Org.Apache.REEF.Network.Tests.NetworkService
{
    public class NetworkServiceTests
    {
        [Fact]
        public void TestNetworkServiceOneWayCommunication()
        {
            int networkServicePort1 = NetworkUtils.GenerateRandomPort(6000, 7000);
            int networkServicePort2 = NetworkUtils.GenerateRandomPort(7001, 8000);

            BlockingCollection<string> queue = new BlockingCollection<string>();

            using (var nameServer = NameServerTests.BuildNameServer())
            {
                IPEndPoint endpoint = nameServer.LocalEndpoint;
                int nameServerPort = endpoint.Port;
                string nameServerAddr = endpoint.Address.ToString();
                using (INetworkService<string> networkService1 = BuildNetworkService(networkServicePort1, nameServerPort, nameServerAddr, null))
                using (INetworkService<string> networkService2 = BuildNetworkService(networkServicePort2, nameServerPort, nameServerAddr, new MessageHandler(queue)))
                {
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

        [Fact]
        public void TestNetworkServiceTwoWayCommunication()
        {
            int networkServicePort1 = NetworkUtils.GenerateRandomPort(6000, 7000);
            int networkServicePort2 = NetworkUtils.GenerateRandomPort(7001, 8000);

            BlockingCollection<string> queue1 = new BlockingCollection<string>();
            BlockingCollection<string> queue2 = new BlockingCollection<string>();

            using (var nameServer = NameServerTests.BuildNameServer())
            {
                IPEndPoint endpoint = nameServer.LocalEndpoint;
                int nameServerPort = endpoint.Port;
                string nameServerAddr = endpoint.Address.ToString();
                using (INetworkService<string> networkService1 = BuildNetworkService(networkServicePort1, nameServerPort, nameServerAddr, new MessageHandler(queue1)))
                using (INetworkService<string> networkService2 = BuildNetworkService(networkServicePort2, nameServerPort, nameServerAddr, new MessageHandler(queue2)))
                {
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
                        connection2.Write("mno");

                        Assert.Equal("abc", queue2.Take());
                        Assert.Equal("def", queue2.Take());
                        Assert.Equal("ghi", queue2.Take());

                        Assert.Equal("jkl", queue1.Take());
                        Assert.Equal("mno", queue1.Take());
                    }
                }
            }
        }

        private INetworkService<string> BuildNetworkService(
            int networkServicePort,
            int nameServicePort,
            string nameServiceAddr,
            IObserver<NsMessage<string>> handler)
        {
            // Test injection
            if (handler == null)
            {
                var networkServiceConf = TangFactory.GetTang().NewConfigurationBuilder()
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
                    .BindImplementation(GenericType<ICodec<string>>.Class, GenericType<StringCodec>.Class)
                    .BindImplementation(GenericType<IObserver<NsMessage<string>>>.Class, GenericType<NetworkMessageHandler>.Class)
                    .Build();

                return TangFactory.GetTang().NewInjector(networkServiceConf).GetInstance<NetworkService<string>>();
            }

            var nameserverConf = TangFactory.GetTang().NewConfigurationBuilder()
             .BindNamedParameter<NamingConfigurationOptions.NameServerPort, int>(
                 GenericType<NamingConfigurationOptions.NameServerPort>.Class,
                 nameServicePort.ToString(CultureInfo.CurrentCulture))
             .BindNamedParameter<NamingConfigurationOptions.NameServerAddress, string>(
                 GenericType<NamingConfigurationOptions.NameServerAddress>.Class,
                 nameServiceAddr)
             .BindImplementation(GenericType<INameClient>.Class, GenericType<NameClient>.Class)
             .Build();
            var injector = TangFactory.GetTang().NewInjector(nameserverConf);
            var nameClient = injector.GetInstance<NameClient>();
            var remoteManagerFactory = injector.GetInstance<IRemoteManagerFactory>();
            return new NetworkService<string>(networkServicePort,
                handler, new StringIdentifierFactory(), new StringCodec(), nameClient, remoteManagerFactory);
        }

        private class MessageHandler : IObserver<NsMessage<string>>
        {
            private readonly BlockingCollection<string> _queue;

            public MessageHandler(BlockingCollection<string> queue)
            {
                _queue = queue;
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

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

using Org.Apache.Reef.Common.io;
using Org.Apache.Reef.IO.Network.Naming;
using Org.Apache.Reef.IO.Network.NetworkService;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Reef.Wake;
using Org.Apache.Reef.Wake.Remote;
using Org.Apache.Reef.Wake.Remote.Impl;
using Org.Apache.Reef.Wake.Util;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.Reef.Test.IO.Tests
{
    [TestClass]
    public class NetworkServiceTests
    {
        [TestMethod]
        public void TestNetworkServiceOneWayCommunication()
        {
            int networkServicePort1 = NetworkUtils.GenerateRandomPort(6000, 7000);
            int networkServicePort2 = NetworkUtils.GenerateRandomPort(7001, 8000);

            BlockingCollection<string> queue = new BlockingCollection<string>();

            using (INameServer nameServer = new NameServer(0))
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

                        Assert.AreEqual("abc", queue.Take());
                        Assert.AreEqual("def", queue.Take());
                        Assert.AreEqual("ghi", queue.Take());
                    }
                }
            }
        }

        [TestMethod]
        public void TestNetworkServiceTwoWayCommunication()
        {
            int networkServicePort1 = NetworkUtils.GenerateRandomPort(6000, 7000);
            int networkServicePort2 = NetworkUtils.GenerateRandomPort(7001, 8000);

            BlockingCollection<string> queue1 = new BlockingCollection<string>();
            BlockingCollection<string> queue2 = new BlockingCollection<string>();

            using (INameServer nameServer = new NameServer(0))
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

                        Assert.AreEqual("abc", queue2.Take());
                        Assert.AreEqual("def", queue2.Take());
                        Assert.AreEqual("ghi", queue2.Take());

                        Assert.AreEqual("jkl", queue1.Take());
                        Assert.AreEqual("mno", queue1.Take());
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
                    .BindImplementation(GenericType<ICodec<string>>.Class, GenericType<StringCodec>.Class)
                    .BindImplementation(GenericType<IObserver<NsMessage<string>>>.Class, GenericType<NetworkMessageHandler>.Class)
                    .Build();

                return TangFactory.GetTang().NewInjector(networkServiceConf).GetInstance<NetworkService<string>>();
            }

            return new NetworkService<string>(networkServicePort, nameServiceAddr, nameServicePort, 
                handler, new StringIdentifierFactory(), new StringCodec());
        }

        private class MessageHandler : IObserver<NsMessage<string>>
        {
            private BlockingCollection<string> _queue;

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

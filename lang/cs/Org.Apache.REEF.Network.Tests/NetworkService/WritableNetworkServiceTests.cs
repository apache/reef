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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Impl;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Network.Tests.NetworkService
{
    /// <summary>
    /// Tests for Writable Network Service
    /// </summary>
    [TestClass]
    [Obsolete("Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295 ", false)]
    public class WritableNetworkServiceTests
    {
        /// <summary>
        /// Tests one way communication between two network services
        /// </summary>
        [TestMethod]
        public void TestWritableNetworkServiceOneWayCommunication()
        {
            int networkServicePort1 = NetworkUtils.GenerateRandomPort(6000, 7000);
            int networkServicePort2 = NetworkUtils.GenerateRandomPort(7001, 8000);

            BlockingCollection<WritableString> queue = new BlockingCollection<WritableString>();

            using (INameServer nameServer = new NameServer(0))
            {
                IPEndPoint endpoint = nameServer.LocalEndpoint;
                int nameServerPort = endpoint.Port;
                string nameServerAddr = endpoint.Address.ToString();
                IIdentifierFactory factory = new StringIdentifierFactory();
                using (INetworkService<WritableString> networkService1 = BuildNetworkService(networkServicePort1, nameServerPort, nameServerAddr, null, null))
                using (INetworkService<WritableString> networkService2 = BuildNetworkService(networkServicePort2, nameServerPort, nameServerAddr, factory, new MessageHandler(queue, factory)))
                {
                    IIdentifier id1 = new StringIdentifier("service1");
                    IIdentifier id2 = new StringIdentifier("service2");
                    networkService1.Register(id1);
                    networkService2.Register(id2);

                    using (IConnection<WritableString> connection = networkService1.NewConnection(id2))
                    {
                        connection.Open();
                        connection.Write(new WritableString("abc"));
                        connection.Write(new WritableString("def"));
                        connection.Write(new WritableString("ghi"));

                        Assert.AreEqual("abc", queue.Take().Data);
                        Assert.AreEqual("def", queue.Take().Data);
                        Assert.AreEqual("ghi", queue.Take().Data);
                    }
                }
            }
        }

        /// <summary>
        /// Tests two way communication between two network services
        /// </summary>
        [TestMethod]
        public void TestWritableNetworkServiceTwoWayCommunication()
        {
            int networkServicePort1 = NetworkUtils.GenerateRandomPort(6000, 7000);
            int networkServicePort2 = NetworkUtils.GenerateRandomPort(7001, 8000);

            BlockingCollection<WritableString> queue1 = new BlockingCollection<WritableString>();
            BlockingCollection<WritableString> queue2 = new BlockingCollection<WritableString>();

            using (INameServer nameServer = new NameServer(0))
            {
                IPEndPoint endpoint = nameServer.LocalEndpoint;
                int nameServerPort = endpoint.Port;
                string nameServerAddr = endpoint.Address.ToString();
                IIdentifierFactory factory = new StringIdentifierFactory();

                using (INetworkService<WritableString> networkService1 = BuildNetworkService(networkServicePort1, nameServerPort, nameServerAddr, factory, new MessageHandler(queue1, factory)))
                using (INetworkService<WritableString> networkService2 = BuildNetworkService(networkServicePort2, nameServerPort, nameServerAddr, factory, new MessageHandler(queue2, factory)))
                {
                    IIdentifier id1 = new StringIdentifier("service1");
                    IIdentifier id2 = new StringIdentifier("service2");
                    networkService1.Register(id1);
                    networkService2.Register(id2);

                    using (IConnection<WritableString> connection1 = networkService1.NewConnection(id2))
                    using (IConnection<WritableString> connection2 = networkService2.NewConnection(id1))
                    {
                        connection1.Open();
                        connection1.Write(new WritableString("abc"));
                        connection1.Write(new WritableString("def"));
                        connection1.Write(new WritableString("ghi"));

                        connection2.Open();
                        connection2.Write(new WritableString("jkl"));
                        connection2.Write(new WritableString("nop"));

                        Assert.AreEqual("abc", queue2.Take().Data);
                        Assert.AreEqual("def", queue2.Take().Data);
                        Assert.AreEqual("ghi", queue2.Take().Data);

                        Assert.AreEqual("jkl", queue1.Take().Data);
                        Assert.AreEqual("nop", queue1.Take().Data);
                    }
                }
            }
        }

        /// <summary>
        /// Creates an instance of network service.
        /// </summary>
        /// <param name="networkServicePort">The port that the NetworkService will listen on</param>
        /// <param name="nameServicePort">The port of the NameServer</param>
        /// <param name="nameServiceAddr">The ip address of the NameServer</param>
        /// <param name="factory">Identifier factory for WritableString</param>
        /// <param name="handler">The observer to handle incoming messages</param>
        /// <returns></returns>
        private INetworkService<WritableString> BuildNetworkService(
            int networkServicePort,
            int nameServicePort,
            string nameServiceAddr,
            IIdentifierFactory factory,
            IObserver<WritableNsMessage<WritableString>> handler)
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
                    .BindImplementation(GenericType<IObserver<WritableNsMessage<WritableString>>>.Class, GenericType<NetworkMessageHandler>.Class)
                    .Build();

                return TangFactory.GetTang().NewInjector(networkServiceConf).GetInstance<WritableNetworkService<WritableString>>();
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
            var remoteManager = injector.GetInstance<WritableRemoteManagerFactory>();
            return new WritableNetworkService<WritableString>(networkServicePort,
                handler, new StringIdentifierFactory(), nameClient, remoteManager);
        }

        /// <summary>
        /// The observer to handle incoming messages for WritableString
        /// </summary>
        private class MessageHandler : IObserver<WritableNsMessage<WritableString>>
        {
            private readonly BlockingCollection<WritableString> _queue;
            private readonly IIdentifierFactory _factory;

            public MessageHandler(BlockingCollection<WritableString> queue, IIdentifierFactory factory)
            {
                _queue = queue;
                _factory = factory;
            }

            public void OnNext(WritableNsMessage<WritableString> value)
            {
                value.ConvertStringToIdentifier(_factory);
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
        /// The network handler to handle incoming Writable NsMessages
        /// </summary>
        private class NetworkMessageHandler : IObserver<WritableNsMessage<WritableString>>
        {
            [Inject]
            public NetworkMessageHandler()
            {
            }

            public void OnNext(WritableNsMessage<WritableString> value)
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

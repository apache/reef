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
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Threading;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Examples.Tasks.StreamingTasks;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.Naming.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Xunit;

namespace Org.Apache.REEF.Network.Tests.NamingService
{
    public class NameServerTests
    {
        [Fact]
        public void TestNameServerNoRequests()
        {
           using (var server = BuildNameServer())
           {
           }
        }

        [Fact]
        public void TestNameServerNoRequestsTwoClients()
        {
           using (var server = BuildNameServer())
           {
               var nameClient = GetNameClientInstance(server.LocalEndpoint);
               var nameClient2 = GetNameClientInstance(server.LocalEndpoint);
               nameClient2.Register("1", new IPEndPoint(IPAddress.Any, 8080));
               nameClient.Lookup("1");
           }
        }

        [Fact]
        public void TestNameServerNoRequestsTwoClients2()
        {
           using (var server = BuildNameServer())
           {
               var nameClient = GetNameClientInstance(server.LocalEndpoint);
               var nameClient2 = GetNameClientInstance(server.LocalEndpoint);
               nameClient2.Register("1", new IPEndPoint(IPAddress.Any, 8080));
               nameClient.Lookup("1");
           }
        }

        [Fact]
        public void TestNameServerMultipleRequestsTwoClients()
        {
           using (var server = BuildNameServer())
           {
               var nameClient = GetNameClientInstance(server.LocalEndpoint);
               var nameClient2 = GetNameClientInstance(server.LocalEndpoint);
               nameClient.Register("1", new IPEndPoint(IPAddress.Any, 8080));
               nameClient2.Lookup("1");
           }
        }

        [Fact]
        public void TestRegister()
        {
            using (INameServer server = BuildNameServer())
            {
                using (INameClient client = BuildNameClient(server.LocalEndpoint))
                {
                    IPEndPoint endpoint1 = new IPEndPoint(IPAddress.Parse("100.0.0.1"), 100);
                    IPEndPoint endpoint2 = new IPEndPoint(IPAddress.Parse("100.0.0.2"), 200);
                    IPEndPoint endpoint3 = new IPEndPoint(IPAddress.Parse("100.0.0.3"), 300);

                    // Check that no endpoints have been registered
                    Assert.Null(client.Lookup("a"));
                    Assert.Null(client.Lookup("b"));
                    Assert.Null(client.Lookup("c"));
                
                    // Register endpoints
                    client.Register("a", endpoint1);
                    client.Register("b", endpoint2);
                    client.Register("c", endpoint3);

                    // Check that they can be looked up correctly
                    Assert.Equal(endpoint1, client.Lookup("a"));
                    Assert.Equal(endpoint2, client.Lookup("b"));
                    Assert.Equal(endpoint3, client.Lookup("c"));
                }
            }
        }

        [Fact]
        public void TestUnregister()
        {
            using (INameServer server = BuildNameServer())
            {
                using (INameClient client = BuildNameClient(server.LocalEndpoint))
                {
                    IPEndPoint endpoint1 = new IPEndPoint(IPAddress.Parse("100.0.0.1"), 100);
                
                    // Register endpoint
                    client.Register("a", endpoint1);

                    // Check that it can be looked up correctly
                    Assert.Equal(endpoint1, client.Lookup("a"));

                    // Unregister endpoints
                    client.Unregister("a");
                    Thread.Sleep(1000);

                    // Make sure they were unregistered correctly
                    Assert.Null(client.Lookup("a"));
                }
            }
        }

        [Fact]
        public void TestLookup()
        {
            using (INameServer server = BuildNameServer())
            {
                using (INameClient client = BuildNameClient(server.LocalEndpoint))
                {
                    IPEndPoint endpoint1 = new IPEndPoint(IPAddress.Parse("100.0.0.1"), 100);
                    IPEndPoint endpoint2 = new IPEndPoint(IPAddress.Parse("100.0.0.2"), 200);
                
                    // Register endpoint1
                    client.Register("a", endpoint1);
                    Assert.Equal(endpoint1, client.Lookup("a"));

                    // Reregister identifer a
                    client.Register("a", endpoint2);
                    Assert.Equal(endpoint2, client.Lookup("a"));
                }
            }
        }

        [Fact]
        public void TestLookupList()
        {
            using (INameServer server = BuildNameServer())
            {
                using (INameClient client = BuildNameClient(server.LocalEndpoint))
                {
                    IPEndPoint endpoint1 = new IPEndPoint(IPAddress.Parse("100.0.0.1"), 100);
                    IPEndPoint endpoint2 = new IPEndPoint(IPAddress.Parse("100.0.0.2"), 200);
                    IPEndPoint endpoint3 = new IPEndPoint(IPAddress.Parse("100.0.0.3"), 300);
                
                    // Register endpoints
                    client.Register("a", endpoint1);
                    client.Register("b", endpoint2);
                    client.Register("c", endpoint3);

                    // Look up both at the same time
                    List<string> ids = new List<string> { "a", "b", "c", "d" };
                    List<NameAssignment> assignments = client.Lookup(ids);

                    // Check that a, b, and c are registered
                    Assert.Equal("a", assignments[0].Identifier);
                    Assert.Equal(endpoint1, assignments[0].Endpoint);
                    Assert.Equal("b", assignments[1].Identifier);
                    Assert.Equal(endpoint2, assignments[1].Endpoint);
                    Assert.Equal("c", assignments[2].Identifier);
                    Assert.Equal(endpoint3, assignments[2].Endpoint);

                    // Check that d is not registered
                    Assert.Equal(3, assignments.Count);
                }
            }
        }

        [Fact]
        public void TestNameClientRestart()
        {
            int oldPort = 6666;
            int newPort = 6662;
            INameServer server = BuildNameServer(oldPort);

            using (INameClient client = BuildNameClient(server.LocalEndpoint))
            {
                IPEndPoint endpoint = new IPEndPoint(IPAddress.Parse("100.0.0.1"), 100);
            
                client.Register("a", endpoint);
                Assert.Equal(endpoint, client.Lookup("a"));

                server.Dispose();

                server = BuildNameServer(newPort);
                client.Restart(server.LocalEndpoint);

                client.Register("b", endpoint);
                Assert.Equal(endpoint, client.Lookup("b"));

                server.Dispose();
            }
        }

        [Fact]
        public void TestConstructorInjection()
        {
            int port = 6666;
            using (INameServer server = BuildNameServer(port))
            {
                IConfiguration nameClientConfiguration = NamingConfiguration.ConfigurationModule
                    .Set(NamingConfiguration.NameServerAddress, server.LocalEndpoint.Address.ToString())
                    .Set(NamingConfiguration.NameServerPort, port + string.Empty)
                    .Build();

                ConstructorInjection c = TangFactory.GetTang()
                    .NewInjector(nameClientConfiguration)
                    .GetInstance<ConstructorInjection>();

                Assert.NotNull(c);
            }
        }

        [Fact]
        public void TestNameCache()
        {
            double interval = 50;
            var config =
                TangFactory.GetTang()
                    .NewConfigurationBuilder()
                    .BindNamedParameter<NameCacheConfiguration.CacheEntryExpiryTime, double>(
                        GenericType<NameCacheConfiguration.CacheEntryExpiryTime>.Class,
                        interval.ToString(CultureInfo.InvariantCulture))
                    .Build();
            
            var injector = TangFactory.GetTang().NewInjector(config);
            var cache = injector.GetInstance<NameCache>();

            cache.Set("dst1", new IPEndPoint(IPAddress.Any, 0));
            Thread.Sleep(100);
            var value = cache.Get("dst1");
            Assert.Null(value);

            IPAddress address = new IPAddress(1234);
            cache.Set("dst1", new IPEndPoint(address, 0));
            value = cache.Get("dst1");
            Assert.NotNull(value);
            Assert.Equal(address, value.Address);
        }

        public static INameServer BuildNameServer(int listenPort = 0)
        {
            var builder = TangFactory.GetTang()
                .NewConfigurationBuilder()
                .BindIntNamedParam<NamingConfigurationOptions.NameServerPort>(listenPort.ToString());

            return TangFactory.GetTang().NewInjector(builder.Build()).GetInstance<INameServer>();
        }

        private INameClient BuildNameClient(IPEndPoint remoteEndpoint)
        {
            string nameServerAddr = remoteEndpoint.Address.ToString();
            int nameServerPort = remoteEndpoint.Port;
            IConfiguration nameClientConfiguration = NamingConfiguration.ConfigurationModule
                .Set(NamingConfiguration.NameServerAddress, nameServerAddr)
                .Set(NamingConfiguration.NameServerPort, nameServerPort + string.Empty)
                .Build();

            return TangFactory.GetTang().NewInjector(nameClientConfiguration).GetInstance<NameClient>();
        }

        private NameClient GetNameClientInstance(IPEndPoint endPoint)
        {
            var config1 = TangFactory.GetTang().NewConfigurationBuilder()
                   .BindNamedParameter<NamingConfigurationOptions.NameServerAddress, string>(
                       GenericType<NamingConfigurationOptions.NameServerAddress>.Class,
                       endPoint.Address.ToString())
                   .BindNamedParameter<NamingConfigurationOptions.NameServerPort, int>(
                       GenericType<NamingConfigurationOptions.NameServerPort>.Class,
                       endPoint.Port.ToString())
                   .Build();

            return TangFactory.GetTang().NewInjector(config1).GetInstance<NameClient>();
        }

        private class ConstructorInjection
        {
            [Inject]
            public ConstructorInjection(NameClient client)
            {
                if (client == null)
                {
                    throw new ArgumentNullException("client");
                }
            }
        }
    }
}

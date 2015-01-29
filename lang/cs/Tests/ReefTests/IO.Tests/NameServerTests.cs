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
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.Reef.Common.io;
using Org.Apache.Reef.IO.Network.Naming;
using Org.Apache.Reef.IO.Network.Naming.Events;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Reef.Wake.Util;

namespace Org.Apache.Reef.Test
{
    [TestClass]
    public class NameServerTests
    {
        [TestMethod]
        public void TestNameServerNoRequests()
        {
           using (var server = new NameServer(0))
           {
           }
        }

        [TestMethod]
        public void TestNameServerNoRequestsTwoClients()
        {
           using (var server = new NameServer(0))
           {
               var nameClient = new NameClient(server.LocalEndpoint);
               var nameClient2 = new NameClient(server.LocalEndpoint);
               nameClient2.Register("1", new IPEndPoint(IPAddress.Any, 8080));
               nameClient.Lookup("1");
           }
        }

        [TestMethod]
        public void TestNameServerNoRequestsTwoClients2()
        {
           using (var server = new NameServer(0))
           {
               var nameClient = new NameClient(server.LocalEndpoint);
               var nameClient2 = new NameClient(server.LocalEndpoint);
               nameClient2.Register("1", new IPEndPoint(IPAddress.Any, 8080));
               nameClient.Lookup("1");
           }
        }

        [TestMethod]
        public void TestNameServerMultipleRequestsTwoClients()
        {
           using (var server = new NameServer(0))
           {
               var nameClient = new NameClient(server.LocalEndpoint);
               var nameClient2 = new NameClient(server.LocalEndpoint);
               nameClient.Register("1", new IPEndPoint(IPAddress.Any, 8080));
               nameClient2.Lookup("1");
           }
        }

        [TestMethod]
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
                    Assert.IsNull(client.Lookup("a"));
                    Assert.IsNull(client.Lookup("b"));
                    Assert.IsNull(client.Lookup("c"));
                
                    // Register endpoints
                    client.Register("a", endpoint1);
                    client.Register("b", endpoint2);
                    client.Register("c", endpoint3);

                    // Check that they can be looked up correctly
                    Assert.AreEqual(endpoint1, client.Lookup("a"));
                    Assert.AreEqual(endpoint2, client.Lookup("b"));
                    Assert.AreEqual(endpoint3, client.Lookup("c"));
                }
            }
        }

        [TestMethod]
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
                    Assert.AreEqual(endpoint1, client.Lookup("a"));

                    // Unregister endpoints
                    client.Unregister("a");
                    Thread.Sleep(1000);

                    // Make sure they were unregistered correctly
                    Assert.IsNull(client.Lookup("a"));
                }
            }
        }

        [TestMethod]
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
                    Assert.AreEqual(endpoint1, client.Lookup("a"));

                    // Reregister identifer a
                    client.Register("a", endpoint2);
                    Assert.AreEqual(endpoint2, client.Lookup("a"));
                }
            }
        }

        [TestMethod]
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
                    Assert.AreEqual("a", assignments[0].Identifier);
                    Assert.AreEqual(endpoint1, assignments[0].Endpoint);
                    Assert.AreEqual("b", assignments[1].Identifier);
                    Assert.AreEqual(endpoint2, assignments[1].Endpoint);
                    Assert.AreEqual("c", assignments[2].Identifier);
                    Assert.AreEqual(endpoint3, assignments[2].Endpoint);

                    // Check that d is not registered
                    Assert.AreEqual(3, assignments.Count);
                }
            }
        }

        [TestMethod]
        public void TestNameClientRestart()
        {
            int oldPort = 6666;
            int newPort = 6662;
            INameServer server = new NameServer(oldPort);

            using (INameClient client = BuildNameClient(server.LocalEndpoint))
            {
                IPEndPoint endpoint = new IPEndPoint(IPAddress.Parse("100.0.0.1"), 100);
            
                client.Register("a", endpoint);
                Assert.AreEqual(endpoint, client.Lookup("a"));

                server.Dispose();

                server = new NameServer(newPort);
                client.Restart(server.LocalEndpoint);

                client.Register("b", endpoint);
                Assert.AreEqual(endpoint, client.Lookup("b"));

                server.Dispose();
            }
        }

        [TestMethod]
        public void TestConstructorInjection()
        {
            int port = 6666;
            using (INameServer server = new NameServer(port))
            {
                IConfiguration nameClientConfiguration = NamingConfiguration.ConfigurationModule
                    .Set(NamingConfiguration.NameServerAddress, server.LocalEndpoint.Address.ToString())
                    .Set(NamingConfiguration.NameServerPort, port + string.Empty)
                    .Build();

                ConstructorInjection c = TangFactory.GetTang()
                    .NewInjector(nameClientConfiguration)
                    .GetInstance<ConstructorInjection>();

                Assert.IsNotNull(c);
            }
        }

        private INameServer BuildNameServer()
        {
            var builder = TangFactory.GetTang()
                                     .NewConfigurationBuilder()
                                     .BindNamedParameter<NamingConfigurationOptions.NameServerPort, int>(
                                         GenericType<NamingConfigurationOptions.NameServerPort>.Class, "0");

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

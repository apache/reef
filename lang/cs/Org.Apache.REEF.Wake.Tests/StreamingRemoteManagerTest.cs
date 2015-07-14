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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Wake.Tests
{
    [TestClass]
    public class StreamingRemoteManagerTest
    {
        private readonly StreamingRemoteManagerFactory _remoteManagerFactory1 =
            TangFactory.GetTang().NewInjector().GetInstance<StreamingRemoteManagerFactory>();

        private readonly StreamingRemoteManagerFactory _remoteManagerFactory2 =
        TangFactory.GetTang().NewInjector().GetInstance<StreamingRemoteManagerFactory>();
        
        /// <summary>
        /// Tests one way communication between Remote Managers 
        /// Remote Manager listens on any available port
        /// </summary>
        [TestMethod]
        public void TestStreamingOneWayCommunication()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<WritableString> queue = new BlockingCollection<WritableString>();
            List<string> events = new List<string>();

            using (var remoteManager1 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            using (var remoteManager2 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            {
                var observer = Observer.Create<WritableString>(queue.Add);
                IPEndPoint endpoint1 = new IPEndPoint(listeningAddress, 0);
                remoteManager2.RegisterObserver(endpoint1, observer);

                var remoteObserver = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                remoteObserver.OnNext(new WritableString("abc"));
                remoteObserver.OnNext(new WritableString("def"));
                remoteObserver.OnNext(new WritableString("ghi"));

                events.Add(queue.Take().Data);
                events.Add(queue.Take().Data);
                events.Add(queue.Take().Data);
            }

            Assert.AreEqual(3, events.Count);
        }

        /// <summary>
        /// Tests two way communications. Checks whether both sides are able to receive messages
        /// </summary>
        [TestMethod]
        public void TestStreamingTwoWayCommunication()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<WritableString> queue1 = new BlockingCollection<WritableString>();
            BlockingCollection<WritableString> queue2 = new BlockingCollection<WritableString>();
            List<string> events1 = new List<string>();
            List<string> events2 = new List<string>();

            using (var remoteManager1 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            using (var remoteManager2 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            {
                // Register observers for remote manager 1 and remote manager 2
                var remoteEndpoint = new IPEndPoint(listeningAddress, 0);
                var observer1 = Observer.Create<WritableString>(queue1.Add);
                var observer2 = Observer.Create<WritableString>(queue2.Add);
                remoteManager1.RegisterObserver(remoteEndpoint, observer1);
                remoteManager2.RegisterObserver(remoteEndpoint, observer2);

                // Remote manager 1 sends 3 events to remote manager 2
                var remoteObserver1 = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                remoteObserver1.OnNext(new WritableString("abc"));
                remoteObserver1.OnNext(new WritableString("def"));
                remoteObserver1.OnNext(new WritableString("ghi"));

                // Remote manager 2 sends 4 events to remote manager 1
                var remoteObserver2 = remoteManager2.GetRemoteObserver(remoteManager1.LocalEndpoint);
                remoteObserver2.OnNext(new WritableString("jkl"));
                remoteObserver2.OnNext(new WritableString("mno"));
                remoteObserver2.OnNext(new WritableString("pqr"));
                remoteObserver2.OnNext(new WritableString("stu"));

                events1.Add(queue1.Take().Data);
                events1.Add(queue1.Take().Data);
                events1.Add(queue1.Take().Data);
                events1.Add(queue1.Take().Data);

                events2.Add(queue2.Take().Data);
                events2.Add(queue2.Take().Data);
                events2.Add(queue2.Take().Data);
            }

            Assert.AreEqual(4, events1.Count);
            Assert.AreEqual(3, events2.Count);
        }

        /// <summary>
        /// Tests one way communication between 3 nodes.
        /// nodes 1 and 2 send messages to node 3
        /// </summary>
        [TestMethod]
        public void TestStreamingCommunicationThreeNodesOneWay()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<WritableString> queue = new BlockingCollection<WritableString>();
            List<string> events = new List<string>();

            using (var remoteManager1 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            using (var remoteManager2 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            using (var remoteManager3 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            {
                var remoteEndpoint = new IPEndPoint(listeningAddress, 0);
                var observer = Observer.Create<WritableString>(queue.Add);
                remoteManager3.RegisterObserver(remoteEndpoint, observer);

                var remoteObserver1 = remoteManager1.GetRemoteObserver(remoteManager3.LocalEndpoint);
                var remoteObserver2 = remoteManager2.GetRemoteObserver(remoteManager3.LocalEndpoint);

                remoteObserver2.OnNext(new WritableString("abc"));
                remoteObserver1.OnNext(new WritableString("def"));
                remoteObserver2.OnNext(new WritableString("ghi"));
                remoteObserver1.OnNext(new WritableString("jkl"));
                remoteObserver2.OnNext(new WritableString("mno"));

                for (int i = 0; i < 5; i++)
                {
                    events.Add(queue.Take().Data);
                }
            }

            Assert.AreEqual(5, events.Count);
        }

        /// <summary>
        /// Tests one way communication between 3 nodes.
        /// nodes 1 and 2 send messages to node 3 and node 3 sends message back
        /// </summary>
        [TestMethod]
        public void TestStreamingCommunicationThreeNodesBothWays()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<WritableString> queue1 = new BlockingCollection<WritableString>();
            BlockingCollection<WritableString> queue2 = new BlockingCollection<WritableString>();
            BlockingCollection<WritableString> queue3 = new BlockingCollection<WritableString>();
            List<string> events1 = new List<string>();
            List<string> events2 = new List<string>();
            List<string> events3 = new List<string>();

            using (var remoteManager1 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            using (var remoteManager2 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            using (var remoteManager3 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            {
                var remoteEndpoint = new IPEndPoint(listeningAddress, 0);

                var observer = Observer.Create<WritableString>(queue1.Add);
                remoteManager1.RegisterObserver(remoteEndpoint, observer);
                var observer2 = Observer.Create<WritableString>(queue2.Add);
                remoteManager2.RegisterObserver(remoteEndpoint, observer2);
                var observer3 = Observer.Create<WritableString>(queue3.Add);
                remoteManager3.RegisterObserver(remoteEndpoint, observer3);

                var remoteObserver1 = remoteManager1.GetRemoteObserver(remoteManager3.LocalEndpoint);
                var remoteObserver2 = remoteManager2.GetRemoteObserver(remoteManager3.LocalEndpoint);

                // Observer 1 and 2 send messages to observer 3
                remoteObserver1.OnNext(new WritableString("abc"));
                remoteObserver1.OnNext(new WritableString("abc"));
                remoteObserver1.OnNext(new WritableString("abc"));
                remoteObserver2.OnNext(new WritableString("def"));
                remoteObserver2.OnNext(new WritableString("def"));

                // Observer 3 sends messages back to observers 1 and 2
                var remoteObserver3A = remoteManager3.GetRemoteObserver(remoteManager1.LocalEndpoint);
                var remoteObserver3B = remoteManager3.GetRemoteObserver(remoteManager2.LocalEndpoint);

                remoteObserver3A.OnNext(new WritableString("ghi"));
                remoteObserver3A.OnNext(new WritableString("ghi"));
                remoteObserver3B.OnNext(new WritableString("jkl"));
                remoteObserver3B.OnNext(new WritableString("jkl"));
                remoteObserver3B.OnNext(new WritableString("jkl"));

                events1.Add(queue1.Take().Data);
                events1.Add(queue1.Take().Data);

                events2.Add(queue2.Take().Data);
                events2.Add(queue2.Take().Data);
                events2.Add(queue2.Take().Data);

                events3.Add(queue3.Take().Data);
                events3.Add(queue3.Take().Data);
                events3.Add(queue3.Take().Data);
                events3.Add(queue3.Take().Data);
                events3.Add(queue3.Take().Data);
            }

            Assert.AreEqual(2, events1.Count);
            Assert.AreEqual(3, events2.Count);
            Assert.AreEqual(5, events3.Count);
        }

        /// <summary>
        /// Tests whether remote manager is able to send acknowledgement back
        /// </summary>
        [TestMethod]
        public void TestStreamingRemoteSenderCallback()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<WritableString> queue = new BlockingCollection<WritableString>();
            List<string> events = new List<string>();

            using (var remoteManager1 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            using (var remoteManager2 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            {
                // Register handler for when remote manager 2 receives events; respond
                // with an ack
                var remoteEndpoint = new IPEndPoint(listeningAddress, 0);
                var remoteObserver2 = remoteManager2.GetRemoteObserver(remoteManager1.LocalEndpoint);

                var receiverObserver = Observer.Create<WritableString>(
                    message => remoteObserver2.OnNext(new WritableString("received message: " + message.Data)));
                remoteManager2.RegisterObserver(remoteEndpoint, receiverObserver);

                // Register handler for remote manager 1 to record the ack
                var senderObserver = Observer.Create<WritableString>(queue.Add);
                remoteManager1.RegisterObserver(remoteEndpoint, senderObserver);

                // Begin to send messages
                var remoteObserver1 = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                remoteObserver1.OnNext(new WritableString("hello"));
                remoteObserver1.OnNext(new WritableString("there"));
                remoteObserver1.OnNext(new WritableString("buddy"));

                events.Add(queue.Take().Data);
                events.Add(queue.Take().Data);
                events.Add(queue.Take().Data);
            }

            Assert.AreEqual(3, events.Count);
            Assert.AreEqual("received message: hello", events[0]);
            Assert.AreEqual("received message: there", events[1]);
            Assert.AreEqual("received message: buddy", events[2]);
        }
        
        /// <summary>
        /// Test whether observer can be created with IRemoteMessage interface
        /// </summary>
        [TestMethod]
        public void TestStreamingRegisterObserverByType()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<WritableString> queue = new BlockingCollection<WritableString>();
            List<string> events = new List<string>();

            using (var remoteManager1 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            using (var remoteManager2 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            {
                // RemoteManager2 listens and records events of type IRemoteEvent<WritableString>
                var observer = Observer.Create<IRemoteMessage<WritableString>>(message => queue.Add(message.Message));
                remoteManager2.RegisterObserver(observer);

                // Remote manager 1 sends 3 events to remote manager 2
                var remoteObserver = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                remoteObserver.OnNext(new WritableString("abc"));
                remoteObserver.OnNext(new WritableString("def"));
                remoteObserver.OnNext(new WritableString("ghi"));

                events.Add(queue.Take().Data);
                events.Add(queue.Take().Data);
                events.Add(queue.Take().Data);
            }

            Assert.AreEqual(3, events.Count);
        }

        /// <summary>
        /// Tests whether we get the cached observer back for sending message without reinstantiating it
        /// </summary>
        [TestMethod]
        public void TestStreamingCachedConnection()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<WritableString> queue = new BlockingCollection<WritableString>();
            List<string> events = new List<string>();

            using (var remoteManager1 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            using (var remoteManager2 = _remoteManagerFactory1.GetInstance<WritableString>(listeningAddress, 0))
            {
                var observer = Observer.Create<WritableString>(queue.Add);
                IPEndPoint endpoint1 = new IPEndPoint(listeningAddress, 0);
                remoteManager2.RegisterObserver(endpoint1, observer);

                var remoteObserver = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                remoteObserver.OnNext(new WritableString("abc"));
                remoteObserver.OnNext(new WritableString("def"));

                var cachedObserver = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                cachedObserver.OnNext(new WritableString("ghi"));
                cachedObserver.OnNext(new WritableString("jkl"));

                events.Add(queue.Take().Data);
                events.Add(queue.Take().Data);
                events.Add(queue.Take().Data);
                events.Add(queue.Take().Data);
            }

            Assert.AreEqual(4, events.Count);
        }
    }
}

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
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Xunit;

namespace Org.Apache.REEF.Wake.Tests
{
    public class RemoteManagerTest
    {
        private readonly IRemoteManagerFactory _remoteManagerFactory =
            TangFactory.GetTang().NewInjector().GetInstance<IRemoteManagerFactory>();
        [Fact]
        public void TestOneWayCommunication()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<string> queue = new BlockingCollection<string>();
            List<string> events = new List<string>();

            using (var remoteManager1 = GetRemoteManager())
            using (var remoteManager2 = GetRemoteManager())
            {
                var observer = Observer.Create<string>(queue.Add);
                IPEndPoint endpoint1 = new IPEndPoint(listeningAddress, 0);
                remoteManager2.RegisterObserver(endpoint1, observer);

                var remoteObserver = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                remoteObserver.OnNext("abc");
                remoteObserver.OnNext("def");
                remoteObserver.OnNext("ghi");

                events.Add(queue.Take());
                events.Add(queue.Take());
                events.Add(queue.Take());
            }

            Assert.Equal(3, events.Count);
        }

        [Fact]
        public void TestOneWayCommunicationClientOnly()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<string> queue = new BlockingCollection<string>();
            List<string> events = new List<string>();

            using (var remoteManager1 = _remoteManagerFactory.GetInstance(new StringCodec()))
            using (var remoteManager2 = _remoteManagerFactory.GetInstance(listeningAddress, new StringCodec()))
            {
                IPEndPoint remoteEndpoint = new IPEndPoint(listeningAddress, 0);
                var observer = Observer.Create<string>(queue.Add);
                remoteManager2.RegisterObserver(remoteEndpoint, observer);

                var remoteObserver = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                remoteObserver.OnNext("abc");
                remoteObserver.OnNext("def");
                remoteObserver.OnNext("ghi");

                events.Add(queue.Take());
                events.Add(queue.Take());
                events.Add(queue.Take());
            }

            Assert.Equal(3, events.Count);
        }

        [Fact]
        public void TestTwoWayCommunication()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<string> queue1 = new BlockingCollection<string>();
            BlockingCollection<string> queue2 = new BlockingCollection<string>();
            List<string> events1 = new List<string>();
            List<string> events2 = new List<string>();

            using (var remoteManager1 = GetRemoteManager())
            using (var remoteManager2 = GetRemoteManager())
            {
                // Register observers for remote manager 1 and remote manager 2
                var remoteEndpoint = new IPEndPoint(listeningAddress, 0);
                var observer1 = Observer.Create<string>(queue1.Add);
                var observer2 = Observer.Create<string>(queue2.Add);
                remoteManager1.RegisterObserver(remoteEndpoint, observer1);
                remoteManager2.RegisterObserver(remoteEndpoint, observer2);

                // Remote manager 1 sends 3 events to remote manager 2
                var remoteObserver1 = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                remoteObserver1.OnNext("abc");
                remoteObserver1.OnNext("def");
                remoteObserver1.OnNext("ghi");

                // Remote manager 2 sends 4 events to remote manager 1
                var remoteObserver2 = remoteManager2.GetRemoteObserver(remoteManager1.LocalEndpoint);
                remoteObserver2.OnNext("jkl");
                remoteObserver2.OnNext("mno");
                remoteObserver2.OnNext("pqr");
                remoteObserver2.OnNext("stu");

                events1.Add(queue1.Take());
                events1.Add(queue1.Take());
                events1.Add(queue1.Take());
                events1.Add(queue1.Take());

                events2.Add(queue2.Take());
                events2.Add(queue2.Take());
                events2.Add(queue2.Take());
            }

            Assert.Equal(4, events1.Count);
            Assert.Equal(3, events2.Count);
        }

        [Fact]
        public void TestCommunicationThreeNodesOneWay()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<string> queue = new BlockingCollection<string>();
            List<string> events = new List<string>();

            using (var remoteManager1 = GetRemoteManager())
            using (var remoteManager2 = GetRemoteManager())
            using (var remoteManager3 = GetRemoteManager())
            {
                var remoteEndpoint = new IPEndPoint(listeningAddress, 0);
                var observer = Observer.Create<string>(queue.Add);
                remoteManager3.RegisterObserver(remoteEndpoint, observer);

                var remoteObserver1 = remoteManager1.GetRemoteObserver(remoteManager3.LocalEndpoint);
                var remoteObserver2 = remoteManager2.GetRemoteObserver(remoteManager3.LocalEndpoint);

                remoteObserver2.OnNext("abc");
                remoteObserver1.OnNext("def");
                remoteObserver2.OnNext("ghi");
                remoteObserver1.OnNext("jkl");
                remoteObserver2.OnNext("mno");

                for (int i = 0; i < 5; i++)
                {
                    events.Add(queue.Take());
                }
            }

            Assert.Equal(5, events.Count);
        }

        [Fact]
        public void TestCommunicationThreeNodesBothWays()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<string> queue1 = new BlockingCollection<string>();
            BlockingCollection<string> queue2 = new BlockingCollection<string>();
            BlockingCollection<string> queue3 = new BlockingCollection<string>();
            List<string> events1 = new List<string>();
            List<string> events2 = new List<string>();
            List<string> events3 = new List<string>();

            using (var remoteManager1 = GetRemoteManager())
            using (var remoteManager2 = GetRemoteManager())
            using (var remoteManager3 = GetRemoteManager())
            {
                var remoteEndpoint = new IPEndPoint(listeningAddress, 0);

                var observer = Observer.Create<string>(queue1.Add);
                remoteManager1.RegisterObserver(remoteEndpoint, observer);
                var observer2 = Observer.Create<string>(queue2.Add);
                remoteManager2.RegisterObserver(remoteEndpoint, observer2);
                var observer3 = Observer.Create<string>(queue3.Add);
                remoteManager3.RegisterObserver(remoteEndpoint, observer3);

                var remoteObserver1 = remoteManager1.GetRemoteObserver(remoteManager3.LocalEndpoint);
                var remoteObserver2 = remoteManager2.GetRemoteObserver(remoteManager3.LocalEndpoint);

                // Observer 1 and 2 send messages to observer 3
                remoteObserver1.OnNext("abc");
                remoteObserver1.OnNext("abc");
                remoteObserver1.OnNext("abc");
                remoteObserver2.OnNext("def");
                remoteObserver2.OnNext("def");

                // Observer 3 sends messages back to observers 1 and 2
                var remoteObserver3a = remoteManager3.GetRemoteObserver(remoteManager1.LocalEndpoint);
                var remoteObserver3b = remoteManager3.GetRemoteObserver(remoteManager2.LocalEndpoint);

                remoteObserver3a.OnNext("ghi");
                remoteObserver3a.OnNext("ghi");
                remoteObserver3b.OnNext("jkl");
                remoteObserver3b.OnNext("jkl");
                remoteObserver3b.OnNext("jkl");

                events1.Add(queue1.Take());
                events1.Add(queue1.Take());

                events2.Add(queue2.Take());
                events2.Add(queue2.Take());
                events2.Add(queue2.Take());

                events3.Add(queue3.Take());
                events3.Add(queue3.Take());
                events3.Add(queue3.Take());
                events3.Add(queue3.Take());
                events3.Add(queue3.Take());
            }

            Assert.Equal(2, events1.Count);
            Assert.Equal(3, events2.Count);
            Assert.Equal(5, events3.Count);
        }

        [Fact]
        public void TestRemoteSenderCallback()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<string> queue = new BlockingCollection<string>();
            List<string> events = new List<string>();

            using (var remoteManager1 = GetRemoteManager())
            using (var remoteManager2 = GetRemoteManager())
            {
                // Register handler for when remote manager 2 receives events; respond
                // with an ack
                var remoteEndpoint = new IPEndPoint(listeningAddress, 0);
                var remoteObserver2 = remoteManager2.GetRemoteObserver(remoteManager1.LocalEndpoint);

                var receiverObserver = Observer.Create<string>(
                    message => remoteObserver2.OnNext("received message: " + message));
                remoteManager2.RegisterObserver(remoteEndpoint, receiverObserver);

                // Register handler for remote manager 1 to record the ack
                var senderObserver = Observer.Create<string>(queue.Add);
                remoteManager1.RegisterObserver(remoteEndpoint, senderObserver);

                // Begin to send messages
                var remoteObserver1 = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                remoteObserver1.OnNext("hello");
                remoteObserver1.OnNext("there");
                remoteObserver1.OnNext("buddy");

                events.Add(queue.Take());
                events.Add(queue.Take());
                events.Add(queue.Take());
            }

            Assert.Equal(3, events.Count);
            Assert.Equal("received message: hello", events[0]);
            Assert.Equal("received message: there", events[1]);
            Assert.Equal("received message: buddy", events[2]);
        }
        
        [Fact]
        public void TestRegisterObserverByType()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<string> queue = new BlockingCollection<string>();
            List<string> events = new List<string>();

            using (var remoteManager1 = GetRemoteManager())
            using (var remoteManager2 = GetRemoteManager())
            {
                // RemoteManager2 listens and records events of type IRemoteEvent<string>
                var observer = Observer.Create<IRemoteMessage<string>>(message => queue.Add(message.Message));
                remoteManager2.RegisterObserver(observer);

                // Remote manager 1 sends 3 events to remote manager 2
                var remoteObserver = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                remoteObserver.OnNext("abc");
                remoteObserver.OnNext("def");
                remoteObserver.OnNext("ghi");

                events.Add(queue.Take());
                events.Add(queue.Take());
                events.Add(queue.Take());
            }

            Assert.Equal(3, events.Count);
        }

        [Fact]
        public void TestCachedConnection()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");

            BlockingCollection<string> queue = new BlockingCollection<string>();
            List<string> events = new List<string>();

            using (var remoteManager1 = GetRemoteManager())
            using (var remoteManager2 = GetRemoteManager())
            {
                var observer = Observer.Create<string>(queue.Add);
                IPEndPoint endpoint1 = new IPEndPoint(listeningAddress, 0);
                remoteManager2.RegisterObserver(endpoint1, observer);

                var remoteObserver = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                remoteObserver.OnNext("abc");
                remoteObserver.OnNext("def");

                var cachedObserver = remoteManager1.GetRemoteObserver(remoteManager2.LocalEndpoint);
                cachedObserver.OnNext("ghi");
                cachedObserver.OnNext("jkl");

                events.Add(queue.Take());
                events.Add(queue.Take());
                events.Add(queue.Take());
                events.Add(queue.Take());
            }

            Assert.Equal(4, events.Count);
        }

        private IRemoteManager<string> GetRemoteManager()
        {
            IPAddress listeningAddress = IPAddress.Parse("127.0.0.1");
            return _remoteManagerFactory.GetInstance(listeningAddress, new StringCodec());
        }
    }
}

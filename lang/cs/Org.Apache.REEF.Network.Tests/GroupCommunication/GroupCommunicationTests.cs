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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reactive;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Codec;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Network.Tests.GroupCommunication
{
    [TestClass]
    public class GroupCommunicationTests
    {
        [TestMethod]
        public void TestSender()
        {
            using (NameServer nameServer = new NameServer(0))
            {
                IPEndPoint endpoint = nameServer.LocalEndpoint;
                BlockingCollection<GroupCommunicationMessage> messages1 = new BlockingCollection<GroupCommunicationMessage>();
                BlockingCollection<GroupCommunicationMessage> messages2 = new BlockingCollection<GroupCommunicationMessage>();

                var handler1 = Observer.Create<NsMessage<GroupCommunicationMessage>>(
                    msg => messages1.Add(msg.Data.First()));
                var handler2 = Observer.Create<NsMessage<GroupCommunicationMessage>>(
                    msg => messages2.Add(msg.Data.First()));

                var networkService1 = BuildNetworkService(endpoint, handler1);
                var networkService2 = BuildNetworkService(endpoint, handler2);

                networkService1.Register(new StringIdentifier("id1"));
                networkService2.Register(new StringIdentifier("id2"));

                Sender sender1 = new Sender(networkService1, new StringIdentifierFactory());
                Sender sender2 = new Sender(networkService2, new StringIdentifierFactory());

                sender1.Send(CreateGcm("abc", "id1", "id2"));
                sender1.Send(CreateGcm("def", "id1", "id2"));

                sender2.Send(CreateGcm("ghi", "id2", "id1"));

                string msg1 = Encoding.UTF8.GetString(messages2.Take().Data[0]);
                string msg2 = Encoding.UTF8.GetString(messages2.Take().Data[0]);
                Assert.AreEqual("abc", msg1);
                Assert.AreEqual("def", msg2);

                string msg3 = Encoding.UTF8.GetString(messages1.Take().Data[0]);
                Assert.AreEqual("ghi", msg3);
            }
        }

        [TestMethod]
        public void TestBroadcastReduceOperators()
        {
            string groupName = "group1";
            string broadcastOperatorName = "broadcast";
            string reduceOperatorName = "reduce";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 3;
            int fanOut = 2;

            var groupCommunicationDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommunicationDriver.DefaultGroup
                .AddBroadcast<int, IntCodec>(
                    broadcastOperatorName,
                    masterTaskId)
                .AddReduce<int, IntCodec>(
                    reduceOperatorName,
                    masterTaskId,
                    new SumFunction())
                .Build();

            var commGroups = CommGroupClients(groupName, numTasks, groupCommunicationDriver, commGroup);

            //for master task
            IBroadcastSender<int> broadcastSender = commGroups[0].GetBroadcastSender<int>(broadcastOperatorName);
            IReduceReceiver<int> sumReducer = commGroups[0].GetReduceReceiver<int>(reduceOperatorName);

            IBroadcastReceiver<int> broadcastReceiver1 = commGroups[1].GetBroadcastReceiver<int>(broadcastOperatorName);
            IReduceSender<int> triangleNumberSender1 = commGroups[1].GetReduceSender<int>(reduceOperatorName);

            IBroadcastReceiver<int> broadcastReceiver2 = commGroups[2].GetBroadcastReceiver<int>(broadcastOperatorName);
            IReduceSender<int> triangleNumberSender2 = commGroups[2].GetReduceSender<int>(reduceOperatorName);

            for (int j = 1; j <= 10; j++)
            {
                broadcastSender.Send(j);

                int n1 = broadcastReceiver1.Receive();
                int n2 = broadcastReceiver2.Receive();
                Assert.AreEqual(j, n1);
                Assert.AreEqual(j, n2);

                int triangleNum1 = TriangleNumber(n1);
                triangleNumberSender1.Send(triangleNum1);
                int triangleNum2 = TriangleNumber(n2);
                triangleNumberSender2.Send(triangleNum2);

                int sum = sumReducer.Reduce();
                int expected = TriangleNumber(j) * (numTasks - 1);
                Assert.AreEqual(sum, expected);
            }
        }
 
        [TestMethod]
        public void TestScatterReduceOperators()
        {
            string groupName = "group1";
            string scatterOperatorName = "scatter";
            string reduceOperatorName = "reduce";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 5;
            int fanOut = 2;

            IGroupCommDriver groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
                .AddScatter<int, IntCodec>(
                    scatterOperatorName,
                    masterTaskId)
                .AddReduce<int, IntCodec>(
                    reduceOperatorName,
                        masterTaskId,
                        new SumFunction())
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(scatterOperatorName);
            IReduceReceiver<int> sumReducer = commGroups[0].GetReduceReceiver<int>(reduceOperatorName);

            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(scatterOperatorName);
            IReduceSender<int> sumSender1 = commGroups[1].GetReduceSender<int>(reduceOperatorName);

            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(scatterOperatorName);
            IReduceSender<int> sumSender2 = commGroups[2].GetReduceSender<int>(reduceOperatorName);

            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(scatterOperatorName);
            IReduceSender<int> sumSender3 = commGroups[3].GetReduceSender<int>(reduceOperatorName);

            IScatterReceiver<int> receiver4 = commGroups[4].GetScatterReceiver<int>(scatterOperatorName);
            IReduceSender<int> sumSender4 = commGroups[4].GetReduceSender<int>(reduceOperatorName);

            Assert.IsNotNull(sender);
            Assert.IsNotNull(receiver1);
            Assert.IsNotNull(receiver2);
            Assert.IsNotNull(receiver3);
            Assert.IsNotNull(receiver4);

            List<int> data = Enumerable.Range(1, 100).ToList();
            List<string> order = new List<string> {"task4", "task3", "task2", "task1"};

            sender.Send(data, order);

            ScatterReceiveReduce(receiver4, sumSender4);
            ScatterReceiveReduce(receiver3, sumSender3);
            ScatterReceiveReduce(receiver2, sumSender2);
            ScatterReceiveReduce(receiver1, sumSender1);

            int sum = sumReducer.Reduce();

            Assert.AreEqual(sum, data.Sum());
        }

        [TestMethod]
        public void TestBroadcastOperator()
        {
            NameServer nameServer = new NameServer(0);

            string groupName = "group1";
            string operatorName = "broadcast";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 10;
            int value = 1337;
            int fanOut = 3;

            IGroupCommDriver groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            var commGroup = groupCommDriver.DefaultGroup
                .AddBroadcast<int, IntCodec>(operatorName, masterTaskId)
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

            IBroadcastSender<int> sender = commGroups[0].GetBroadcastSender<int>(operatorName);
            IBroadcastReceiver<int> receiver1 = commGroups[1].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver2 = commGroups[2].GetBroadcastReceiver<int>(operatorName);

            Assert.IsNotNull(sender);
            Assert.IsNotNull(receiver1);
            Assert.IsNotNull(receiver2);

            sender.Send(value);
            Assert.AreEqual(value, receiver1.Receive());
            Assert.AreEqual(value, receiver2.Receive());
        }

        [TestMethod]
        public void TestBroadcastOperatorWithDefaultCodec()
        {
            NameServer nameServer = new NameServer(0);

            string groupName = "group1";
            string operatorName = "broadcast";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 10;
            int value = 1337;
            int fanOut = 3;

            IGroupCommDriver groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            var commGroup = groupCommDriver.DefaultGroup
                .AddBroadcast(operatorName, masterTaskId)
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

            IBroadcastSender<int> sender = commGroups[0].GetBroadcastSender<int>(operatorName);
            IBroadcastReceiver<int> receiver1 = commGroups[1].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver2 = commGroups[2].GetBroadcastReceiver<int>(operatorName);

            Assert.IsNotNull(sender);
            Assert.IsNotNull(receiver1);
            Assert.IsNotNull(receiver2);

            sender.Send(value);
            Assert.AreEqual(value, receiver1.Receive());
            Assert.AreEqual(value, receiver2.Receive());
        }

        [TestMethod]
        public void TestBroadcastOperator2()
        {
            string groupName = "group1";
            string operatorName = "broadcast";
            string driverId = "driverId";
            string masterTaskId = "task0";
            int numTasks = 3;
            int value1 = 1337;
            int value2 = 42;
            int value3 = 99;
            int fanOut = 2;

            IGroupCommDriver groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            var commGroup = groupCommDriver.DefaultGroup
              .AddBroadcast<int, IntCodec>(operatorName, masterTaskId)
              .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

            IBroadcastSender<int> sender = commGroups[0].GetBroadcastSender<int>(operatorName);
            IBroadcastReceiver<int> receiver1 = commGroups[1].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver2 = commGroups[2].GetBroadcastReceiver<int>(operatorName);

            Assert.IsNotNull(sender);
            Assert.IsNotNull(receiver1);
            Assert.IsNotNull(receiver2);

            sender.Send(value1);
            Assert.AreEqual(value1, receiver1.Receive());
            Assert.AreEqual(value1, receiver2.Receive());

            sender.Send(value2);
            Assert.AreEqual(value2, receiver1.Receive());
            Assert.AreEqual(value2, receiver2.Receive());

            sender.Send(value3);
            Assert.AreEqual(value3, receiver1.Receive());
            Assert.AreEqual(value3, receiver2.Receive());
        }

        [TestMethod]
        public void TestReduceOperator()
        {
            string groupName = "group1";
            string operatorName = "reduce";
            int numTasks = 4;
            string driverId = "driverid";
            string masterTaskId = "task0";
            int fanOut = 2;

            IGroupCommDriver groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            var commGroup = groupCommDriver.DefaultGroup
                .AddReduce<int, IntCodec>(operatorName, "task0", new SumFunction())
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

            IReduceReceiver<int> receiver = commGroups[0].GetReduceReceiver<int>(operatorName);
            IReduceSender<int> sender1 = commGroups[1].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender2 = commGroups[2].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender3 = commGroups[3].GetReduceSender<int>(operatorName);

            Assert.IsNotNull(receiver);
            Assert.IsNotNull(sender1);
            Assert.IsNotNull(sender2);
            Assert.IsNotNull(sender3);

            sender3.Send(5);
            sender1.Send(1);
            sender2.Send(3);

            Assert.AreEqual(9, receiver.Reduce());
        }

        [TestMethod]
        public void TestReduceOperator2()
        {
            string groupName = "group1";
            string operatorName = "reduce";
            int numTasks = 4;
            string driverId = "driverid";
            string masterTaskId = "task0";
            int fanOut = 2;

            IGroupCommDriver groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            var commGroup = groupCommDriver.DefaultGroup
                .AddReduce<int, IntCodec>(operatorName, "task0", new SumFunction())
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

            IReduceReceiver<int> receiver = commGroups[0].GetReduceReceiver<int>(operatorName);
            IReduceSender<int> sender1 = commGroups[1].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender2 = commGroups[2].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender3 = commGroups[3].GetReduceSender<int>(operatorName);

            Assert.IsNotNull(receiver);
            Assert.IsNotNull(sender1);
            Assert.IsNotNull(sender2);
            Assert.IsNotNull(sender3);

            sender3.Send(5);
            sender1.Send(1);
            sender2.Send(3);
            Assert.AreEqual(9, receiver.Reduce());

            sender3.Send(6);
            sender1.Send(2);
            sender2.Send(4);
            Assert.AreEqual(12, receiver.Reduce());

            sender3.Send(9);
            sender1.Send(3);
            sender2.Send(6);
            Assert.AreEqual(18, receiver.Reduce());
        }

        [TestMethod]
        public void TestScatterOperator()
        {
            string groupName = "group1";
            string operatorName = "scatter";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 5;
            int fanOut = 2;

            IGroupCommDriver groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            var commGroup = groupCommDriver.DefaultGroup
                .AddScatter(operatorName, masterTaskId)
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(operatorName);
            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver4 = commGroups[4].GetScatterReceiver<int>(operatorName);

            Assert.IsNotNull(sender);
            Assert.IsNotNull(receiver1);
            Assert.IsNotNull(receiver2);
            Assert.IsNotNull(receiver3);
            Assert.IsNotNull(receiver4);

            List<int> data = new List<int> { 1, 2, 3, 4 };

            sender.Send(data);
            Assert.AreEqual(1, receiver1.Receive().Single());
            Assert.AreEqual(2, receiver2.Receive().Single());
            Assert.AreEqual(3, receiver3.Receive().Single());
            Assert.AreEqual(4, receiver4.Receive().Single());
        }

        [TestMethod]
        public void TestScatterOperatorWithDefaultCodec()
        {
            string groupName = "group1";
            string operatorName = "scatter";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 5;
            int fanOut = 2;

            IGroupCommDriver groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            var commGroup = groupCommDriver.DefaultGroup
                .AddScatter(operatorName, masterTaskId)
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(operatorName);
            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver4 = commGroups[4].GetScatterReceiver<int>(operatorName);

            Assert.IsNotNull(sender);
            Assert.IsNotNull(receiver1);
            Assert.IsNotNull(receiver2);
            Assert.IsNotNull(receiver3);
            Assert.IsNotNull(receiver4);

            List<int> data = new List<int> { 1, 2, 3, 4 };

            sender.Send(data);
            Assert.AreEqual(1, receiver1.Receive().Single());
            Assert.AreEqual(2, receiver2.Receive().Single());
            Assert.AreEqual(3, receiver3.Receive().Single());
            Assert.AreEqual(4, receiver4.Receive().Single());
        }

        [TestMethod]
        public void TestScatterOperator2()
        {
            string groupName = "group1";
            string operatorName = "scatter";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 5;
            int fanOut = 2;

            IGroupCommDriver groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            var commGroup = groupCommDriver.DefaultGroup
                .AddScatter<int, IntCodec>(operatorName, masterTaskId)
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(operatorName);
            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver4 = commGroups[4].GetScatterReceiver<int>(operatorName);

            Assert.IsNotNull(sender);
            Assert.IsNotNull(receiver1);
            Assert.IsNotNull(receiver2);
            Assert.IsNotNull(receiver3);
            Assert.IsNotNull(receiver4);

            List<int> data = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8 };

            sender.Send(data);
            var data1 = receiver1.Receive();
            Assert.AreEqual(1, data1.First());
            Assert.AreEqual(2, data1.Last());

            var data2 = receiver2.Receive();
            Assert.AreEqual(3, data2.First());
            Assert.AreEqual(4, data2.Last());

            var data3 = receiver3.Receive();
            Assert.AreEqual(5, data3.First());
            Assert.AreEqual(6, data3.Last());

            var data4 = receiver4.Receive();
            Assert.AreEqual(7, data4.First());
            Assert.AreEqual(8, data4.Last());
        }

        [TestMethod]
        public void TestScatterOperator3()
        {
            string groupName = "group1";
            string operatorName = "scatter";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 4;
            int fanOut = 2;

            IGroupCommDriver groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            var commGroup = groupCommDriver.DefaultGroup
                .AddScatter<int, IntCodec>(operatorName, masterTaskId)
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(operatorName);
            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(operatorName);

            Assert.IsNotNull(sender);
            Assert.IsNotNull(receiver1);
            Assert.IsNotNull(receiver2);
            Assert.IsNotNull(receiver3);

            List<int> data = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8 };

            sender.Send(data);

            var data1 = receiver1.Receive().ToArray();
            Assert.AreEqual(1, data1[0]);
            Assert.AreEqual(2, data1[1]);
            Assert.AreEqual(3, data1[2]);

            var data2 = receiver2.Receive().ToArray();
            Assert.AreEqual(4, data2[0]);
            Assert.AreEqual(5, data2[1]);
            Assert.AreEqual(6, data2[2]);

            var data3 = receiver3.Receive().ToArray();
            Assert.AreEqual(7, data3[0]);
            Assert.AreEqual(8, data3[1]);
        }

        [TestMethod]
        public void TestScatterOperator4()
        {
            string groupName = "group1";
            string operatorName = "scatter";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 4;
            int fanOut = 2;

            IGroupCommDriver groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            var commGroup = groupCommDriver.DefaultGroup
                .AddScatter<int, IntCodec>(operatorName, masterTaskId)
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);
            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(operatorName);
            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(operatorName);

            Assert.IsNotNull(sender);
            Assert.IsNotNull(receiver1);
            Assert.IsNotNull(receiver2);
            Assert.IsNotNull(receiver3);

            List<int> data = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8 };
            List<string> order = new List<string> { "task3", "task2", "task1" };

            sender.Send(data, order);

            var data3 = receiver3.Receive().ToArray();
            Assert.AreEqual(1, data3[0]);
            Assert.AreEqual(2, data3[1]);
            Assert.AreEqual(3, data3[2]);

            var data2 = receiver2.Receive().ToArray();
            Assert.AreEqual(4, data2[0]);
            Assert.AreEqual(5, data2[1]);
            Assert.AreEqual(6, data2[2]);

            var data1 = receiver1.Receive().ToArray();
            Assert.AreEqual(7, data1[0]);
            Assert.AreEqual(8, data1[1]);
        }

        [TestMethod]
        public void TestConfigurationBroadcastSpec()
        {
            FlatTopology<int, IntCodec> topology = new FlatTopology<int, IntCodec>("Operator", "Operator", "task1", "driverid",
                new BroadcastOperatorSpec<int, IntCodec>("Sender"));

            topology.AddTask("task1");
            var conf = topology.GetTaskConfiguration("task1");

            ICodec<int> codec = TangFactory.GetTang().NewInjector(conf).GetInstance<ICodec<int>>();
            Assert.AreEqual(3, codec.Decode(codec.Encode(3)));
        }

        [TestMethod]
        public void TestConfigurationReduceSpec()
        {
            FlatTopology<int, IntCodec> topology = new FlatTopology<int, IntCodec>("Operator", "Group", "task1", "driverid",
                new ReduceOperatorSpec<int, IntCodec>("task1", new SumFunction()));

            topology.AddTask("task1");
            var conf2 = topology.GetTaskConfiguration("task1");

            IReduceFunction<int> reduceFunction = TangFactory.GetTang().NewInjector(conf2).GetInstance<IReduceFunction<int>>();
            Assert.AreEqual(10, reduceFunction.Reduce(new int[] { 1, 2, 3, 4 }));
        }

        public static IGroupCommDriver GetInstanceOfGroupCommDriver(string driverId, string masterTaskId, string groupName, int fanOut, int numTasks)
        {
            var c = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<GroupCommConfigurationOptions.DriverId>(driverId)
                .BindStringNamedParam<GroupCommConfigurationOptions.MasterTaskId>(masterTaskId)
                .BindStringNamedParam<GroupCommConfigurationOptions.GroupName>(groupName)
                .BindIntNamedParam<GroupCommConfigurationOptions.FanOut>(fanOut.ToString())
                .BindIntNamedParam<GroupCommConfigurationOptions.NumberOfTasks>(numTasks.ToString())
                .BindImplementation(GenericType<IConfigurationSerializer>.Class, GenericType<AvroConfigurationSerializer>.Class)
                .Build();

            IGroupCommDriver groupCommDriver = TangFactory.GetTang().NewInjector(c).GetInstance<GroupCommDriver>();
            return groupCommDriver;
        }

        public static List<ICommunicationGroupClient> CommGroupClients(string groupName, int numTasks, IGroupCommDriver groupCommDriver, ICommunicationGroupDriver commGroup)
        {
            List<ICommunicationGroupClient> commGroups = new List<ICommunicationGroupClient>();
            IConfiguration serviceConfig = groupCommDriver.GetServiceConfiguration();

            List<IConfiguration> partialConfigs = new List<IConfiguration>();
            for (int i = 0; i < numTasks; i++)
            {
                string taskId = "task" + i;
                IConfiguration partialTaskConfig = TangFactory.GetTang().NewConfigurationBuilder(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, taskId)
                        .Set(TaskConfiguration.Task, GenericType<MyTask>.Class)
                        .Build())
                    .Build();
                commGroup.AddTask(taskId);
                partialConfigs.Add(partialTaskConfig);
            }

            for (int i = 0; i < numTasks; i++)
            {
                string taskId = "task" + i;
                IConfiguration groupCommTaskConfig = groupCommDriver.GetGroupCommTaskConfiguration(taskId);
                IConfiguration mergedConf = Configurations.Merge(groupCommTaskConfig, partialConfigs[i], serviceConfig);
                IInjector injector = TangFactory.GetTang().NewInjector(mergedConf);

                IGroupCommClient groupCommClient = injector.GetInstance<IGroupCommClient>();
                commGroups.Add(groupCommClient.GetCommunicationGroup(groupName));
            }
            return commGroups;
        }

        public static NetworkService<GroupCommunicationMessage> BuildNetworkService(
            IPEndPoint nameServerEndpoint, IObserver<NsMessage<GroupCommunicationMessage>> handler)
        {
            return new NetworkService<GroupCommunicationMessage>(
                0, handler, new StringIdentifierFactory(), new GroupCommunicationMessageCodec(), new NameClient(nameServerEndpoint.Address.ToString(), nameServerEndpoint.Port));
        }

        private GroupCommunicationMessage CreateGcm(string message, string from, string to)
        {
            byte[] data = Encoding.UTF8.GetBytes(message);
            return new GroupCommunicationMessage("g1", "op1", from, to, data, MessageType.Data);
        }

        private static void ScatterReceiveReduce(IScatterReceiver<int> receiver, IReduceSender<int> sumSender)
        {
            List<int> data1 = receiver.Receive();
            int sum1 = data1.Sum();
            sumSender.Send(sum1);
        }

        public static int TriangleNumber(int n)
        {
            return Enumerable.Range(1, n).Sum();
        }
    }

    public class SumFunction : IReduceFunction<int>
    {
        [Inject]
        public SumFunction()
        {
        }

        public int Reduce(IEnumerable<int> elements)
        {
            return elements.Sum();
        }
    }

    public class MyTask : ITask
    {
        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public byte[] Call(byte[] memento)
        {
            throw new NotImplementedException();
        }
    }
}
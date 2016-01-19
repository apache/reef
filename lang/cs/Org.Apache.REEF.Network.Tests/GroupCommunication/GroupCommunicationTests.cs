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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Reactive;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Examples.GroupCommunication;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Network.Tests.NamingService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.StreamingCodec;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;
using Xunit;

namespace Org.Apache.REEF.Network.Tests.GroupCommunication
{
    public class GroupCommunicationTests
    {
        [Fact]
        public void TestSender()
        {
            using (var nameServer = NameServerTests.BuildNameServer())
            {
                IPEndPoint endpoint = nameServer.LocalEndpoint;

                BlockingCollection<GeneralGroupCommunicationMessage> messages1 =
                    new BlockingCollection<GeneralGroupCommunicationMessage>();
                BlockingCollection<GeneralGroupCommunicationMessage> messages2 =
                    new BlockingCollection<GeneralGroupCommunicationMessage>();

                var handler1 =
                    Observer.Create<NsMessage<GeneralGroupCommunicationMessage>>(msg => messages1.Add(msg.Data.First()));
                var handler2 =
                    Observer.Create<NsMessage<GeneralGroupCommunicationMessage>>(msg => messages2.Add(msg.Data.First()));

                var networkServiceInjector1 = BuildNetworkServiceInjector(endpoint, handler1);
                var networkServiceInjector2 = BuildNetworkServiceInjector(endpoint, handler2);

                var networkService1 = 
                    networkServiceInjector1.GetInstance<StreamingNetworkService<GeneralGroupCommunicationMessage>>();
                var networkService2 = 
                    networkServiceInjector2.GetInstance<StreamingNetworkService<GeneralGroupCommunicationMessage>>();
                networkService1.Register(new StringIdentifier("id1"));
                networkService2.Register(new StringIdentifier("id2"));

                Sender sender1 = networkServiceInjector1.GetInstance<Sender>();
                Sender sender2 = networkServiceInjector2.GetInstance<Sender>();

                sender1.Send(CreateGcmStringType("abc", "id1", "id2"));
                sender1.Send(CreateGcmStringType("def", "id1", "id2"));
                sender2.Send(CreateGcmStringType("ghi", "id2", "id1"));

                string msg1 = (messages2.Take() as GroupCommunicationMessage<string>).Data[0];
                string msg2 = (messages2.Take() as GroupCommunicationMessage<string>).Data[0];

                Assert.Equal("abc", msg1);
                Assert.Equal("def", msg2);

                string msg3 = (messages1.Take() as GroupCommunicationMessage<string>).Data[0];
                Assert.Equal("ghi", msg3);
            }
        }

        [Fact]
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
                .AddBroadcast<int>(
                    broadcastOperatorName,
                    masterTaskId,
                    TopologyTypes.Flat,
                    GetDefaultDataConverterConfig())
                .AddReduce<int>(
                    reduceOperatorName,
                    masterTaskId,
                    TopologyTypes.Flat,
                    GetDefaultDataConverterConfig(),
                    GetDefaultReduceFuncConfig())
                .Build();

            var commGroups = CommGroupClients(groupName, numTasks, groupCommunicationDriver, commGroup, GetDefaultCodecConfig());

            // for master task
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
                Assert.Equal(j, n1);
                Assert.Equal(j, n2);

                int triangleNum1 = TriangleNumber(n1);
                triangleNumberSender1.Send(triangleNum1);
                int triangleNum2 = TriangleNumber(n2);
                triangleNumberSender2.Send(triangleNum2);

                int sum = sumReducer.Reduce();
                int expected = TriangleNumber(j) * (numTasks - 1);
                Assert.Equal(sum, expected);
            }
        }

        /// <summary>
        /// This is to test operator injection in CommunicationGroupClient with int[] as message type
        /// </summary>
        [Fact]
        public void TestGetBroadcastReduceOperatorsForIntArrayMessageType()
        {
            const string groupName = "group1";
            const string broadcastOperatorName = "broadcast";
            const string reduceOperatorName = "reduce";
            const string masterTaskId = "task0";
            const string driverId = "Driver Id";
            const int numTasks = 3;
            const int fanOut = 2;

            IConfiguration codecConfig = StreamingCodecConfiguration<int[]>.Conf
                .Set(StreamingCodecConfiguration<int[]>.Codec, GenericType<IntArrayStreamingCodec>.Class)
                .Build();

            IConfiguration reduceFunctionConfig = ReduceFunctionConfiguration<int[]>.Conf
                .Set(ReduceFunctionConfiguration<int[]>.ReduceFunction, GenericType<ArraySumFunction>.Class)
                .Build();

            IConfiguration dataConverterConfig = TangFactory.GetTang().NewConfigurationBuilder(
                PipelineDataConverterConfiguration<int[]>.Conf
                    .Set(PipelineDataConverterConfiguration<int[]>.DataConverter,
                        GenericType<PipelineIntDataConverter>.Class)
                    .Build())
                .BindNamedParameter<GroupTestConfig.ChunkSize, int>(
                    GenericType<GroupTestConfig.ChunkSize>.Class,
                    GroupTestConstants.ChunkSize.ToString(CultureInfo.InvariantCulture))
                .Build();

            var groupCommunicationDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);
            ICommunicationGroupDriver commGroup = groupCommunicationDriver.DefaultGroup
                    .AddBroadcast<int[]>(
                        broadcastOperatorName,
                        masterTaskId,
                        TopologyTypes.Flat,
                        dataConverterConfig)
                    .AddReduce<int[]>(
                        reduceOperatorName,
                        masterTaskId,
                        TopologyTypes.Flat,
                        dataConverterConfig,
                        reduceFunctionConfig)
                    .Build();

            var commGroups = CommGroupClients(groupName, numTasks, groupCommunicationDriver, commGroup, codecConfig);

            // for master task
            IBroadcastSender<int[]> broadcastSender = commGroups[0].GetBroadcastSender<int[]>(broadcastOperatorName);
            IReduceReceiver<int[]> sumReducer = commGroups[0].GetReduceReceiver<int[]>(reduceOperatorName);

            IBroadcastReceiver<int[]> broadcastReceiver1 = commGroups[1].GetBroadcastReceiver<int[]>(broadcastOperatorName);
            IReduceSender<int[]> triangleNumberSender1 = commGroups[1].GetReduceSender<int[]>(reduceOperatorName);

            IBroadcastReceiver<int[]> broadcastReceiver2 = commGroups[2].GetBroadcastReceiver<int[]>(broadcastOperatorName);
            IReduceSender<int[]> triangleNumberSender2 = commGroups[2].GetReduceSender<int[]>(reduceOperatorName);

            Assert.NotNull(broadcastSender);
            Assert.NotNull(sumReducer);
            Assert.NotNull(broadcastReceiver1);
            Assert.NotNull(triangleNumberSender1);
            Assert.NotNull(broadcastReceiver2);
            Assert.NotNull(triangleNumberSender2);
        }

        [Fact]
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
                .AddScatter<int>(
                    scatterOperatorName,
                    masterTaskId,
                    TopologyTypes.Flat,
                    GetDefaultDataConverterConfig())
                .AddReduce<int>(
                        reduceOperatorName,
                        masterTaskId,
                        TopologyTypes.Flat,
                        GetDefaultReduceFuncConfig(),
                        GetDefaultDataConverterConfig())
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

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

            Assert.NotNull(sender);
            Assert.NotNull(receiver1);
            Assert.NotNull(receiver2);
            Assert.NotNull(receiver3);
            Assert.NotNull(receiver4);

            List<int> data = Enumerable.Range(1, 100).ToList();
            List<string> order = new List<string> { "task4", "task3", "task2", "task1" };

            sender.Send(data, order);

            ScatterReceiveReduce(receiver4, sumSender4);
            ScatterReceiveReduce(receiver3, sumSender3);
            ScatterReceiveReduce(receiver2, sumSender2);
            ScatterReceiveReduce(receiver1, sumSender1);

            int sum = sumReducer.Reduce();

            Assert.Equal(sum, data.Sum());
        }

        [Fact]
        public void TestBroadcastOperator()
        {
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

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IBroadcastSender<int> sender = commGroups[0].GetBroadcastSender<int>(operatorName);
            IBroadcastReceiver<int> receiver1 = commGroups[1].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver2 = commGroups[2].GetBroadcastReceiver<int>(operatorName);

            Assert.NotNull(sender);
            Assert.NotNull(receiver1);
            Assert.NotNull(receiver2);

            sender.Send(value);
            Assert.Equal(value, receiver1.Receive());
            Assert.Equal(value, receiver2.Receive());
        }

        [Fact]
        public void TestBroadcastOperatorWithDefaultCodec()
        {
            INameServer nameServer = NameServerTests.BuildNameServer();

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

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IBroadcastSender<int> sender = commGroups[0].GetBroadcastSender<int>(operatorName);
            IBroadcastReceiver<int> receiver1 = commGroups[1].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver2 = commGroups[2].GetBroadcastReceiver<int>(operatorName);

            Assert.NotNull(sender);
            Assert.NotNull(receiver1);
            Assert.NotNull(receiver2);

            sender.Send(value);
            Assert.Equal(value, receiver1.Receive());
            Assert.Equal(value, receiver2.Receive());
        }

        [Fact]
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
              .AddBroadcast(operatorName, masterTaskId)
              .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IBroadcastSender<int> sender = commGroups[0].GetBroadcastSender<int>(operatorName);
            IBroadcastReceiver<int> receiver1 = commGroups[1].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver2 = commGroups[2].GetBroadcastReceiver<int>(operatorName);

            Assert.NotNull(sender);
            Assert.NotNull(receiver1);
            Assert.NotNull(receiver2);

            sender.Send(value1);
            Assert.Equal(value1, receiver1.Receive());
            Assert.Equal(value1, receiver2.Receive());

            sender.Send(value2);
            Assert.Equal(value2, receiver1.Receive());
            Assert.Equal(value2, receiver2.Receive());

            sender.Send(value3);
            Assert.Equal(value3, receiver1.Receive());
            Assert.Equal(value3, receiver2.Receive());
        }

        [Fact]
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
                .AddReduce<int>(operatorName, "task0", TopologyTypes.Flat, GetDefaultDataConverterConfig(), GetDefaultReduceFuncConfig())
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IReduceReceiver<int> receiver = commGroups[0].GetReduceReceiver<int>(operatorName);
            IReduceSender<int> sender1 = commGroups[1].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender2 = commGroups[2].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender3 = commGroups[3].GetReduceSender<int>(operatorName);

            Assert.NotNull(receiver);
            Assert.NotNull(sender1);
            Assert.NotNull(sender2);
            Assert.NotNull(sender3);

            sender3.Send(5);
            sender1.Send(1);
            sender2.Send(3);

            Assert.Equal(9, receiver.Reduce());
        }

        [Fact]
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
                .AddReduce<int>(operatorName, "task0", TopologyTypes.Flat, GetDefaultDataConverterConfig(), GetDefaultReduceFuncConfig())
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IReduceReceiver<int> receiver = commGroups[0].GetReduceReceiver<int>(operatorName);
            IReduceSender<int> sender1 = commGroups[1].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender2 = commGroups[2].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender3 = commGroups[3].GetReduceSender<int>(operatorName);

            Assert.NotNull(receiver);
            Assert.NotNull(sender1);
            Assert.NotNull(sender2);
            Assert.NotNull(sender3);

            sender3.Send(5);
            sender1.Send(1);
            sender2.Send(3);
            Assert.Equal(9, receiver.Reduce());

            sender3.Send(6);
            sender1.Send(2);
            sender2.Send(4);
            Assert.Equal(12, receiver.Reduce());

            sender3.Send(9);
            sender1.Send(3);
            sender2.Send(6);
            Assert.Equal(18, receiver.Reduce());
        }

        [Fact]
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

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(operatorName);
            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver4 = commGroups[4].GetScatterReceiver<int>(operatorName);

            Assert.NotNull(sender);
            Assert.NotNull(receiver1);
            Assert.NotNull(receiver2);
            Assert.NotNull(receiver3);
            Assert.NotNull(receiver4);

            List<int> data = new List<int> { 1, 2, 3, 4 };

            sender.Send(data);
            Assert.Equal(1, receiver1.Receive().Single());
            Assert.Equal(2, receiver2.Receive().Single());
            Assert.Equal(3, receiver3.Receive().Single());
            Assert.Equal(4, receiver4.Receive().Single());
        }

        [Fact]
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

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(operatorName);
            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver4 = commGroups[4].GetScatterReceiver<int>(operatorName);

            Assert.NotNull(sender);
            Assert.NotNull(receiver1);
            Assert.NotNull(receiver2);
            Assert.NotNull(receiver3);
            Assert.NotNull(receiver4);

            List<int> data = new List<int> { 1, 2, 3, 4 };

            sender.Send(data);
            Assert.Equal(1, receiver1.Receive().Single());
            Assert.Equal(2, receiver2.Receive().Single());
            Assert.Equal(3, receiver3.Receive().Single());
            Assert.Equal(4, receiver4.Receive().Single());
        }

        [Fact]
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
                .AddScatter<int>(operatorName, masterTaskId, TopologyTypes.Flat, GetDefaultDataConverterConfig(), GetDefaultReduceFuncConfig())
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(operatorName);
            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver4 = commGroups[4].GetScatterReceiver<int>(operatorName);

            Assert.NotNull(sender);
            Assert.NotNull(receiver1);
            Assert.NotNull(receiver2);
            Assert.NotNull(receiver3);
            Assert.NotNull(receiver4);

            List<int> data = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8 };

            sender.Send(data);
            var data1 = receiver1.Receive();
            Assert.Equal(1, data1.First());
            Assert.Equal(2, data1.Last());

            var data2 = receiver2.Receive();
            Assert.Equal(3, data2.First());
            Assert.Equal(4, data2.Last());

            var data3 = receiver3.Receive();
            Assert.Equal(5, data3.First());
            Assert.Equal(6, data3.Last());

            var data4 = receiver4.Receive();
            Assert.Equal(7, data4.First());
            Assert.Equal(8, data4.Last());
        }

        [Fact]
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
                .AddScatter<int>(operatorName, masterTaskId, TopologyTypes.Flat, GetDefaultDataConverterConfig())
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(operatorName);
            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(operatorName);

            Assert.NotNull(sender);
            Assert.NotNull(receiver1);
            Assert.NotNull(receiver2);
            Assert.NotNull(receiver3);

            List<int> data = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8 };

            sender.Send(data);

            var data1 = receiver1.Receive().ToArray();
            Assert.Equal(1, data1[0]);
            Assert.Equal(2, data1[1]);
            Assert.Equal(3, data1[2]);

            var data2 = receiver2.Receive().ToArray();
            Assert.Equal(4, data2[0]);
            Assert.Equal(5, data2[1]);
            Assert.Equal(6, data2[2]);

            var data3 = receiver3.Receive().ToArray();
            Assert.Equal(7, data3[0]);
            Assert.Equal(8, data3[1]);
        }

        [Fact]
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
                .AddScatter<int>(operatorName, masterTaskId, TopologyTypes.Flat, GetDefaultDataConverterConfig())
                .Build();

            List<ICommunicationGroupClient> commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());
            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(operatorName);
            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(operatorName);

            Assert.NotNull(sender);
            Assert.NotNull(receiver1);
            Assert.NotNull(receiver2);
            Assert.NotNull(receiver3);

            List<int> data = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8 };
            List<string> order = new List<string> { "task3", "task2", "task1" };

            sender.Send(data, order);

            var data3 = receiver3.Receive().ToArray();
            Assert.Equal(1, data3[0]);
            Assert.Equal(2, data3[1]);
            Assert.Equal(3, data3[2]);

            var data2 = receiver2.Receive().ToArray();
            Assert.Equal(4, data2[0]);
            Assert.Equal(5, data2[1]);
            Assert.Equal(6, data2[2]);

            var data1 = receiver1.Receive().ToArray();
            Assert.Equal(7, data1[0]);
            Assert.Equal(8, data1[1]);
        }

        [Fact]
        public void TestConfigurationBroadcastSpec()
        {
            FlatTopology<int> topology = new FlatTopology<int>("Operator", "Operator", "task1", "driverid",
                new BroadcastOperatorSpec("Sender", GetDefaultCodecConfig(), GetDefaultDataConverterConfig()));

            topology.AddTask("task1");
            var conf = topology.GetTaskConfiguration("task1");

            IStreamingCodec<int> codec = TangFactory.GetTang().NewInjector(conf).GetInstance<IStreamingCodec<int>>();
            
            var stream = new MemoryStream();
            IDataWriter writer = new StreamDataWriter(stream);
            codec.Write(3, writer);
            stream.Position = 0;
            IDataReader reader = new StreamDataReader(stream);
            int res = codec.Read(reader);
            Assert.Equal(3, res);
        }

        [Fact]
        public void TestConfigurationReduceSpec()
        {
            FlatTopology<int> topology = new FlatTopology<int>("Operator", "Group", "task1", "driverid",
                new ReduceOperatorSpec("task1", Configurations.Merge(GetDefaultCodecConfig(), GetDefaultDataConverterConfig(),  GetDefaultReduceFuncConfig())));

            topology.AddTask("task1");
            var conf2 = topology.GetTaskConfiguration("task1");

            IReduceFunction<int> reduceFunction = TangFactory.GetTang().NewInjector(conf2).GetInstance<IReduceFunction<int>>();
            Assert.Equal(10, reduceFunction.Reduce(new int[] { 1, 2, 3, 4 }));
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

        [Fact]
        public async Task TestCodecToStreamingCodecConfiguration()
        {
            var config = CodecToStreamingCodecConfiguration<int>.Conf
               .Set(CodecToStreamingCodecConfiguration<int>.Codec, GenericType<IntCodec>.Class)
               .Build();

            IStreamingCodec<PipelineMessage<int>> streamingCodec =
                TangFactory.GetTang().NewInjector(config).GetInstance<IStreamingCodec<PipelineMessage<int>>>();

            CancellationToken token = new CancellationToken();

            int obj = 5;
            PipelineMessage<int> message = new PipelineMessage<int>(obj, true);
            var stream = new MemoryStream();
            IDataWriter writer = new StreamDataWriter(stream);
            streamingCodec.Write(message, writer);
            PipelineMessage<int> message1 = new PipelineMessage<int>(obj + 1, false);
            await streamingCodec.WriteAsync(message1, writer, token);

            stream.Position = 0;
            IDataReader reader = new StreamDataReader(stream);
            var res1 = streamingCodec.Read(reader);
            var res2 = await streamingCodec.ReadAsync(reader, token);
            Assert.Equal(obj, res1.Data);
            Assert.Equal(obj + 1, res2.Data);
            Assert.Equal(true, res1.IsLast);
            Assert.Equal(false, res2.IsLast);
        }

        public static List<ICommunicationGroupClient> CommGroupClients(string groupName, int numTasks, IGroupCommDriver groupCommDriver, ICommunicationGroupDriver commGroupDriver, IConfiguration userServiceConfig)
        {
            List<ICommunicationGroupClient> commGroups = new List<ICommunicationGroupClient>();
            IConfiguration serviceConfig = groupCommDriver.GetServiceConfiguration();
            serviceConfig = Configurations.Merge(serviceConfig, userServiceConfig);

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
                commGroupDriver.AddTask(taskId);
                partialConfigs.Add(partialTaskConfig);
            }

            for (int i = 0; i < numTasks; i++)
            {
                // get task configuration at driver side
                string taskId = "task" + i;
                IConfiguration groupCommTaskConfig = groupCommDriver.GetGroupCommTaskConfiguration(taskId);
                IConfiguration mergedConf = Configurations.Merge(groupCommTaskConfig, partialConfigs[i], serviceConfig);

                var conf = TangFactory.GetTang()
                    .NewConfigurationBuilder(mergedConf)
                    .BindNamedParameter(typeof(GroupCommConfigurationOptions.Initialize), "false")
                    .Build();
                IInjector injector = TangFactory.GetTang().NewInjector(conf);

                // simulate injection at evaluator side
                IGroupCommClient groupCommClient = injector.GetInstance<IGroupCommClient>();
                commGroups.Add(groupCommClient.GetCommunicationGroup(groupName));
            }
            return commGroups;
        }

        public static IInjector BuildNetworkServiceInjector(
            IPEndPoint nameServerEndpoint, IObserver<NsMessage<GeneralGroupCommunicationMessage>> handler)
        {
            var config = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter(typeof(NamingConfigurationOptions.NameServerAddress),
                    nameServerEndpoint.Address.ToString())
                .BindNamedParameter(typeof(NamingConfigurationOptions.NameServerPort),
                    nameServerEndpoint.Port.ToString())
                .BindNamedParameter(typeof(NetworkServiceOptions.NetworkServicePort),
                    0.ToString(CultureInfo.InvariantCulture))
                .BindImplementation(GenericType<INameClient>.Class, GenericType<NameClient>.Class)
                .Build();

            var codecConfig = StreamingCodecConfiguration<string>.Conf
                .Set(StreamingCodecConfiguration<string>.Codec, GenericType<StringStreamingCodec>.Class)
                .Build();

            config = Configurations.Merge(config, codecConfig);

            var injector = TangFactory.GetTang().NewInjector(config);
            injector.BindVolatileInstance(
                GenericType<IObserver<NsMessage<GeneralGroupCommunicationMessage>>>.Class, handler);

            return injector;
        }

        private GroupCommunicationMessage<string> CreateGcmStringType(string message, string from, string to)
        {
            return new GroupCommunicationMessage<string>("g1", "op1", from, to, message);
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

        private static IConfiguration GetDefaultCodecConfig()
        {
            return StreamingCodecConfiguration<int>.Conf
                .Set(StreamingCodecConfiguration<int>.Codec, GenericType<IntStreamingCodec>.Class)
                .Build();
        }

        private static IConfiguration GetDefaultReduceFuncConfig()
        {
            return ReduceFunctionConfiguration<int>.Conf
                .Set(ReduceFunctionConfiguration<int>.ReduceFunction, GenericType<SumFunction>.Class)
                .Build();
        }

        private static IConfiguration GetDefaultDataConverterConfig()
        {
            return PipelineDataConverterConfiguration<int>.Conf
                .Set(PipelineDataConverterConfiguration<int>.DataConverter, GenericType<DefaultPipelineDataConverter<int>>.Class)
                .Build();
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

    class ArraySumFunction : IReduceFunction<int[]>
    {
        [Inject]
        private ArraySumFunction()
        {
        }

        public int[] Reduce(IEnumerable<int[]> elements)
        {
            int[] result = null;
            int count = 0;

            foreach (var element in elements)
            {
                if (count == 0)
                {
                    result = element.Clone() as int[];
                }
                else
                {
                    if (element.Length != result.Length)
                    {
                        throw new Exception("integer arrays are of different sizes");
                    }

                    for (int i = 0; i < result.Length; i++)
                    {
                        result[i] += element[i];
                    }
                }
                count++;
            }

            return result;
        }
    }

    class PipelineIntDataConverter : IPipelineDataConverter<int[]>
    {
        readonly int _chunkSize;

        [Inject]
        private PipelineIntDataConverter([Parameter(typeof(GroupTestConfig.ChunkSize))] int chunkSize)
        {
            _chunkSize = chunkSize;
        }

        public List<PipelineMessage<int[]>> PipelineMessage(int[] message)
        {
            List<PipelineMessage<int[]>> messageList = new List<PipelineMessage<int[]>>();
            int totalChunks = message.Length / _chunkSize;

            if (message.Length % _chunkSize != 0)
            {
                totalChunks++;
            }

            int counter = 0;
            for (int i = 0; i < message.Length; i += _chunkSize)
            {
                int[] data = new int[Math.Min(_chunkSize, message.Length - i)];
                Buffer.BlockCopy(message, i * sizeof(int), data, 0, data.Length * sizeof(int));

                messageList.Add(counter == totalChunks - 1
                    ? new PipelineMessage<int[]>(data, true)
                    : new PipelineMessage<int[]>(data, false));

                counter++;
            }

            return messageList;
        }

        public int[] FullMessage(List<PipelineMessage<int[]>> pipelineMessage)
        {
            int size = pipelineMessage.Select(x => x.Data.Length).Sum();
            int[] data = new int[size];
            int offset = 0;

            foreach (var message in pipelineMessage)
            {
                Buffer.BlockCopy(message.Data, 0, data, offset, message.Data.Length * sizeof(int));
                offset += message.Data.Length * sizeof(int);
            }

            return data;
        }
    }
}
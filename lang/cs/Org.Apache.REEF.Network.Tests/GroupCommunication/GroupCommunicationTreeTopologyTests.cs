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

using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;
using Xunit;

namespace Org.Apache.REEF.Network.Tests.GroupCommunication
{
    public class GroupCommunicationTreeTopologyTests
    {
        [Fact]
        public void TestTreeTopology()
        {
            TreeTopology<int> topology = new TreeTopology<int>("Operator", "Operator", "task1", "driverid",
                new BroadcastOperatorSpec("task1", GetDefaultDataConverterConfig()), 2);
            for (int i = 1; i < 8; i++)
            {
                string taskid = "task" + i;
                topology.AddTask(taskid);
            }

            for (int i = 1; i < 8; i++)
            {
                var conf = topology.GetTaskConfiguration("task" + i);
            }
        }

        [Fact]
        public void TestReduceOperator()
        {
            string groupName = "group1";
            string operatorName = "reduce";
            int numTasks = 10;
            string driverId = "driverId";
            string masterTaskId = "task0";
            int fanOut = 3;

            var groupCommDriver = GroupCommunicationTests.GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
                .AddReduce<int>(operatorName, masterTaskId, TopologyTypes.Tree, GetDefaultDataConverterConfig(), GetDefaultReduceFuncConfig())
                .Build();

            var commGroups = GroupCommunicationTests.CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IReduceReceiver<int> receiver = commGroups[0].GetReduceReceiver<int>(operatorName);
            IReduceSender<int> sender1 = commGroups[1].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender2 = commGroups[2].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender3 = commGroups[3].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender4 = commGroups[4].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender5 = commGroups[5].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender6 = commGroups[6].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender7 = commGroups[7].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender8 = commGroups[8].GetReduceSender<int>(operatorName);
            IReduceSender<int> sender9 = commGroups[9].GetReduceSender<int>(operatorName);

            Assert.NotNull(receiver);
            Assert.NotNull(sender1);
            Assert.NotNull(sender2);
            Assert.NotNull(sender3);
            Assert.NotNull(sender4);
            Assert.NotNull(sender5);
            Assert.NotNull(sender6);
            Assert.NotNull(sender7);
            Assert.NotNull(sender8);
            Assert.NotNull(sender9);

            sender9.Send(9);
            sender8.Send(8);
            sender7.Send(7);
            sender6.Send(6);
            sender5.Send(5);
            sender4.Send(4);
            sender3.Send(3);
            sender2.Send(2);
            sender1.Send(1);

            Assert.Equal(45, receiver.Reduce());
        }

        [Fact]
        public void TestBroadcastOperator()
        {
            string groupName = "group1";
            string operatorName = "broadcast";
            string driverId = "driverId";
            string masterTaskId = "task0";
            int numTasks = 10;
            int value1 = 1337;
            int value2 = 42;
            int value3 = 99;
            int fanOut = 3;

            var groupCommDriver = GroupCommunicationTests.GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
                .AddBroadcast<int>(operatorName, masterTaskId, TopologyTypes.Tree, GetDefaultDataConverterConfig(), GetDefaultReduceFuncConfig())
                .Build();

            var commGroups = GroupCommunicationTests.CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IBroadcastSender<int> sender = commGroups[0].GetBroadcastSender<int>(operatorName);
            IBroadcastReceiver<int> receiver1 = commGroups[1].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver2 = commGroups[2].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver3 = commGroups[3].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver4 = commGroups[4].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver5 = commGroups[5].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver6 = commGroups[6].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver7 = commGroups[7].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver8 = commGroups[8].GetBroadcastReceiver<int>(operatorName);
            IBroadcastReceiver<int> receiver9 = commGroups[9].GetBroadcastReceiver<int>(operatorName);

            Assert.NotNull(sender);
            Assert.NotNull(receiver1);
            Assert.NotNull(receiver2);
            Assert.NotNull(receiver3);
            Assert.NotNull(receiver4);
            Assert.NotNull(receiver5);
            Assert.NotNull(receiver6);
            Assert.NotNull(receiver7);
            Assert.NotNull(receiver8);
            Assert.NotNull(receiver9);

            sender.Send(value1);
            Assert.Equal(value1, receiver1.Receive());
            Assert.Equal(value1, receiver2.Receive());
            Assert.Equal(value1, receiver3.Receive());
            Assert.Equal(value1, receiver4.Receive());
            Assert.Equal(value1, receiver5.Receive());
            Assert.Equal(value1, receiver6.Receive());
            Assert.Equal(value1, receiver7.Receive());
            Assert.Equal(value1, receiver8.Receive());
            Assert.Equal(value1, receiver9.Receive());

            sender.Send(value2);
            Assert.Equal(value2, receiver1.Receive());
            Assert.Equal(value2, receiver2.Receive());
            Assert.Equal(value2, receiver3.Receive());
            Assert.Equal(value2, receiver4.Receive());
            Assert.Equal(value2, receiver5.Receive());
            Assert.Equal(value2, receiver6.Receive());
            Assert.Equal(value2, receiver7.Receive());
            Assert.Equal(value2, receiver8.Receive());
            Assert.Equal(value2, receiver9.Receive());

            sender.Send(value3);
            Assert.Equal(value3, receiver1.Receive());
            Assert.Equal(value3, receiver2.Receive());
            Assert.Equal(value3, receiver3.Receive());
            Assert.Equal(value3, receiver4.Receive());
            Assert.Equal(value3, receiver5.Receive());
            Assert.Equal(value3, receiver6.Receive());
            Assert.Equal(value3, receiver7.Receive());
            Assert.Equal(value3, receiver8.Receive());
            Assert.Equal(value3, receiver9.Receive());
        }


        [Fact]
        public void TestBroadcastReduceOperators()
        {
            string groupName = "group1";
            string broadcastOperatorName = "broadcast";
            string reduceOperatorName = "reduce";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 10;
            int fanOut = 3;

            var groupCommDriver = GroupCommunicationTests.GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
                .AddBroadcast<int>(
                    broadcastOperatorName,
                    masterTaskId,
                    TopologyTypes.Tree,
                    GetDefaultDataConverterConfig())
                .AddReduce<int>(
                    reduceOperatorName,
                    masterTaskId,
                    TopologyTypes.Tree,
                    GetDefaultDataConverterConfig(),
                    GetDefaultReduceFuncConfig())
                .Build();

            var commGroups = GroupCommunicationTests.CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            // for master task
            IBroadcastSender<int> broadcastSender = commGroups[0].GetBroadcastSender<int>(broadcastOperatorName);
            IReduceReceiver<int> sumReducer = commGroups[0].GetReduceReceiver<int>(reduceOperatorName);

            IBroadcastReceiver<int> broadcastReceiver1 = commGroups[1].GetBroadcastReceiver<int>(broadcastOperatorName);
            IReduceSender<int> triangleNumberSender1 = commGroups[1].GetReduceSender<int>(reduceOperatorName);

            IBroadcastReceiver<int> broadcastReceiver2 = commGroups[2].GetBroadcastReceiver<int>(broadcastOperatorName);
            IReduceSender<int> triangleNumberSender2 = commGroups[2].GetReduceSender<int>(reduceOperatorName);

            IBroadcastReceiver<int> broadcastReceiver3 = commGroups[3].GetBroadcastReceiver<int>(broadcastOperatorName);
            IReduceSender<int> triangleNumberSender3 = commGroups[3].GetReduceSender<int>(reduceOperatorName);

            IBroadcastReceiver<int> broadcastReceiver4 = commGroups[4].GetBroadcastReceiver<int>(broadcastOperatorName);
            IReduceSender<int> triangleNumberSender4 = commGroups[4].GetReduceSender<int>(reduceOperatorName);

            IBroadcastReceiver<int> broadcastReceiver5 = commGroups[5].GetBroadcastReceiver<int>(broadcastOperatorName);
            IReduceSender<int> triangleNumberSender5 = commGroups[5].GetReduceSender<int>(reduceOperatorName);

            IBroadcastReceiver<int> broadcastReceiver6 = commGroups[6].GetBroadcastReceiver<int>(broadcastOperatorName);
            IReduceSender<int> triangleNumberSender6 = commGroups[6].GetReduceSender<int>(reduceOperatorName);

            IBroadcastReceiver<int> broadcastReceiver7 = commGroups[7].GetBroadcastReceiver<int>(broadcastOperatorName);
            IReduceSender<int> triangleNumberSender7 = commGroups[7].GetReduceSender<int>(reduceOperatorName);

            IBroadcastReceiver<int> broadcastReceiver8 = commGroups[8].GetBroadcastReceiver<int>(broadcastOperatorName);
            IReduceSender<int> triangleNumberSender8 = commGroups[8].GetReduceSender<int>(reduceOperatorName);

            IBroadcastReceiver<int> broadcastReceiver9 = commGroups[9].GetBroadcastReceiver<int>(broadcastOperatorName);
            IReduceSender<int> triangleNumberSender9 = commGroups[9].GetReduceSender<int>(reduceOperatorName);

            for (int i = 1; i <= 10; i++)
            {
                broadcastSender.Send(i);

                int n1 = broadcastReceiver1.Receive();
                int n2 = broadcastReceiver2.Receive();
                int n3 = broadcastReceiver3.Receive();
                int n4 = broadcastReceiver4.Receive();
                int n5 = broadcastReceiver5.Receive();
                int n6 = broadcastReceiver6.Receive();
                int n7 = broadcastReceiver7.Receive();
                int n8 = broadcastReceiver8.Receive();
                int n9 = broadcastReceiver9.Receive();
                Assert.Equal(i, n1);
                Assert.Equal(i, n2);
                Assert.Equal(i, n3);
                Assert.Equal(i, n4);
                Assert.Equal(i, n5);
                Assert.Equal(i, n6);
                Assert.Equal(i, n7);
                Assert.Equal(i, n8);
                Assert.Equal(i, n9);

                int triangleNum9 = GroupCommunicationTests.TriangleNumber(n9);
                triangleNumberSender9.Send(triangleNum9);

                int triangleNum8 = GroupCommunicationTests.TriangleNumber(n8);
                triangleNumberSender8.Send(triangleNum8);

                int triangleNum7 = GroupCommunicationTests.TriangleNumber(n7);
                triangleNumberSender7.Send(triangleNum7);

                int triangleNum6 = GroupCommunicationTests.TriangleNumber(n6);
                triangleNumberSender6.Send(triangleNum6);

                int triangleNum5 = GroupCommunicationTests.TriangleNumber(n5);
                triangleNumberSender5.Send(triangleNum5);

                int triangleNum4 = GroupCommunicationTests.TriangleNumber(n4);
                triangleNumberSender4.Send(triangleNum4);

                int triangleNum3 = GroupCommunicationTests.TriangleNumber(n3);
                triangleNumberSender3.Send(triangleNum3);

                int triangleNum2 = GroupCommunicationTests.TriangleNumber(n2);
                triangleNumberSender2.Send(triangleNum2);

                int triangleNum1 = GroupCommunicationTests.TriangleNumber(n1);
                triangleNumberSender1.Send(triangleNum1);

                int sum = sumReducer.Reduce();
                int expected = GroupCommunicationTests.TriangleNumber(i) * (numTasks - 1);
                Assert.Equal(sum, expected);
            }
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

            var groupCommDriver = GroupCommunicationTests.GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
                .AddScatter<int>(operatorName, masterTaskId, TopologyTypes.Tree, GetDefaultDataConverterConfig())
                .Build();

            var commGroups = GroupCommunicationTests.CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

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
            var receved1 = receiver1.Receive().ToArray();
            Assert.Equal(1, receved1[0]);
            Assert.Equal(2, receved1[1]);

            var receved2 = receiver2.Receive().ToArray();
            Assert.Equal(3, receved2[0]);
            Assert.Equal(4, receved2[1]);

            Assert.Equal(1, receiver3.Receive().Single());
            Assert.Equal(2, receiver4.Receive().Single());
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

            var groupCommDriver = GroupCommunicationTests.GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
                .AddScatter<int>(operatorName, masterTaskId, TopologyTypes.Tree, GetDefaultDataConverterConfig())
                .Build();

            var commGroups = GroupCommunicationTests.CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

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
            var data1 = receiver1.Receive().ToArray();
            Assert.Equal(1, data1[0]);
            Assert.Equal(2, data1[1]);
            Assert.Equal(3, data1[2]);
            Assert.Equal(4, data1[3]);

            var data2 = receiver2.Receive().ToArray();
            Assert.Equal(5, data2[0]);
            Assert.Equal(6, data2[1]);
            Assert.Equal(7, data2[2]);
            Assert.Equal(8, data2[3]);

            var data3 = receiver3.Receive();
            Assert.Equal(1, data3.First());
            Assert.Equal(2, data3.Last());

            var data4 = receiver4.Receive();
            Assert.Equal(3, data4.First());
            Assert.Equal(4, data4.Last());
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

            var groupCommDriver = GroupCommunicationTests.GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
                .AddScatter<int>(operatorName, masterTaskId, TopologyTypes.Tree, GetDefaultDataConverterConfig())
                .Build();

            var commGroups = GroupCommunicationTests.CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

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
            Assert.Equal(4, data1[3]);

            var data2 = receiver2.Receive().ToArray();
            Assert.Equal(5, data2[0]);
            Assert.Equal(6, data2[1]);
            Assert.Equal(7, data2[2]);
            Assert.Equal(8, data2[3]);

            var data3 = receiver3.Receive().ToArray();
            Assert.Equal(1, data3[0]);
            Assert.Equal(2, data3[1]);
            Assert.Equal(3, data3[2]);
            Assert.Equal(4, data3[3]);
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

            var groupCommDriver = GroupCommunicationTests.GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
                .AddScatter<int>(operatorName, masterTaskId, TopologyTypes.Tree, GetDefaultDataConverterConfig())
                .Build();

            var commGroups = GroupCommunicationTests.CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(operatorName);
            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(operatorName);

            Assert.NotNull(sender);
            Assert.NotNull(receiver1);
            Assert.NotNull(receiver2);
            Assert.NotNull(receiver3);

            List<int> data = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8 };
            List<string> order = new List<string> { "task2", "task1" };

            sender.Send(data, order);

            var data2 = receiver2.Receive().ToArray();
            Assert.Equal(1, data2[0]);
            Assert.Equal(2, data2[1]);
            Assert.Equal(3, data2[2]);
            Assert.Equal(4, data2[3]);

            var data1 = receiver1.Receive().ToArray();
            Assert.Equal(5, data1[0]);
            Assert.Equal(6, data1[1]);
            Assert.Equal(7, data1[2]);
            Assert.Equal(8, data1[3]);

            var data3 = receiver3.Receive().ToArray();
            Assert.Equal(5, data3[0]);
            Assert.Equal(6, data3[1]);
            Assert.Equal(7, data3[2]);
            Assert.Equal(8, data3[3]);
        }

        [Fact]
        public void TestScatterOperator5()
        {
            string groupName = "group1";
            string operatorName = "scatter";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 6;
            int fanOut = 2;

            var groupCommDriver = GroupCommunicationTests.GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
                .AddScatter<int>(operatorName, masterTaskId, TopologyTypes.Tree, GetDefaultDataConverterConfig())
                .Build();

            var commGroups = GroupCommunicationTests.CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

            IScatterSender<int> sender = commGroups[0].GetScatterSender<int>(operatorName);
            IScatterReceiver<int> receiver1 = commGroups[1].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver2 = commGroups[2].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver3 = commGroups[3].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver4 = commGroups[4].GetScatterReceiver<int>(operatorName);
            IScatterReceiver<int> receiver5 = commGroups[5].GetScatterReceiver<int>(operatorName);

            Assert.NotNull(sender);
            Assert.NotNull(receiver1);
            Assert.NotNull(receiver2);
            Assert.NotNull(receiver3);
            Assert.NotNull(receiver4);
            Assert.NotNull(receiver5);

            List<int> data = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8 };

            sender.Send(data);

            var data1 = receiver1.Receive().ToArray();
            Assert.Equal(1, data1[0]);
            Assert.Equal(2, data1[1]);
            Assert.Equal(3, data1[2]);
            Assert.Equal(4, data1[3]);

            var data2 = receiver2.Receive().ToArray();
            Assert.Equal(5, data2[0]);
            Assert.Equal(6, data2[1]);
            Assert.Equal(7, data2[2]);
            Assert.Equal(8, data2[3]);

            var data3 = receiver3.Receive().ToArray();
            Assert.Equal(1, data3[0]);
            Assert.Equal(2, data3[1]);
            
            var data4 = receiver4.Receive().ToArray();
            Assert.Equal(3, data4[0]);
            Assert.Equal(4, data4[1]);

            var data5 = receiver5.Receive().ToArray();
            Assert.Equal(5, data5[0]);
            Assert.Equal(6, data5[1]);
            Assert.Equal(7, data5[2]);
            Assert.Equal(8, data5[3]);
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

            var groupCommDriver = GroupCommunicationTests.GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);
            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
              .AddScatter<int>(
                    scatterOperatorName,
                    masterTaskId,
                    TopologyTypes.Tree,
                    GetDefaultDataConverterConfig())
                .AddReduce<int>(
                    reduceOperatorName,
                    masterTaskId,
                    TopologyTypes.Tree,
                    GetDefaultDataConverterConfig(),
                    GetDefaultReduceFuncConfig())
                .Build();

            var commGroups = GroupCommunicationTests.CommGroupClients(groupName, numTasks, groupCommDriver, commGroup, GetDefaultCodecConfig());

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

            sender.Send(data);

            List<int> data1 = receiver1.Receive();
            List<int> data2 = receiver2.Receive();

            List<int> data3 = receiver3.Receive();
            List<int> data4 = receiver4.Receive();

            int sum3 = data3.Sum();
            sumSender3.Send(sum3);

            int sum4 = data4.Sum();
            sumSender4.Send(sum4);

            int sum2 = data2.Sum();
            sumSender2.Send(sum2);

            int sum1 = data1.Sum();
            sumSender1.Send(sum1);

            int sum = sumReducer.Reduce();
            Assert.Equal(sum, 6325);
        }

        private IConfiguration GetDefaultCodecConfig()
        {
            return StreamingCodecConfiguration<int>.Conf
                .Set(StreamingCodecConfiguration<int>.Codec, GenericType<IntStreamingCodec>.Class)
                .Build();
        }

        private IConfiguration GetDefaultReduceFuncConfig()
        {
            return ReduceFunctionConfiguration<int>.Conf
                .Set(ReduceFunctionConfiguration<int>.ReduceFunction, GenericType<SumFunction>.Class)
                .Build();
        }

        private IConfiguration GetDefaultDataConverterConfig()
        {
            return PipelineDataConverterConfiguration<int>.Conf
                .Set(PipelineDataConverterConfiguration<int>.DataConverter, GenericType<DefaultPipelineDataConverter<int>>.Class)
                .Build();
        }
    }
}

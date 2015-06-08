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
using System.Globalization;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Network.Group.Task.Impl;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Network.StreamingCodec;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Network.Tests.GroupCommunication
{
    /// <summary>
    /// Test Writable versions of communication operators
    /// </summary>
    [TestClass]
    public class WritableGroupCommunicationTests
    {
        /// <summary>
        /// Tests reduce operator
        /// </summary>
        [TestMethod]
        public void TestWritableReduceOperator()
        {
            string groupName = "group1";
            string operatorName = "reduce";
            int numTasks = 10;
            string driverId = "driverId";
            string masterTaskId = "task0";
            int fanOut = 3;

            var groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
                .AddReduce<int>(operatorName, masterTaskId, TopologyTypes.Tree, GetDefaulDataConverterConfig(), GetDefaulReduceFuncConfig())
                .Build();

            var commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

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
            
            Assert.IsNotNull(receiver);
            Assert.IsNotNull(sender1);
            Assert.IsNotNull(sender2);
            Assert.IsNotNull(sender3);
            Assert.IsNotNull(sender4);
            Assert.IsNotNull(sender5);
            Assert.IsNotNull(sender6);
            Assert.IsNotNull(sender7);
            Assert.IsNotNull(sender8);
            Assert.IsNotNull(sender9);

            sender9.Send(9);
            sender8.Send(8);
            sender7.Send(7);
            sender6.Send(6);
            sender5.Send(5);
            sender4.Send(4);
            sender3.Send(3);
            sender2.Send(2);
            sender1.Send(1);

            int val = receiver.Reduce();
            Assert.AreEqual(45, val);
        }

        /// <summary>
        /// Tests broadcast operator
        /// </summary>
        [TestMethod]
        public void TestWritableBroadcastOperator()
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

            var groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
                .AddBroadcast<int>(operatorName, masterTaskId, TopologyTypes.Tree, GetDefaulDataConverterConfig(), GetDefaulReduceFuncConfig())
                .Build();

            var commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

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

            Assert.IsNotNull(sender);
            Assert.IsNotNull(receiver1);
            Assert.IsNotNull(receiver2);
            Assert.IsNotNull(receiver3);
            Assert.IsNotNull(receiver4);
            Assert.IsNotNull(receiver5);
            Assert.IsNotNull(receiver6);
            Assert.IsNotNull(receiver7);
            Assert.IsNotNull(receiver8);
            Assert.IsNotNull(receiver9);

            sender.Send(value1);
            Assert.AreEqual(value1, receiver1.Receive());
            Assert.AreEqual(value1, receiver2.Receive());
            Assert.AreEqual(value1, receiver3.Receive());
            Assert.AreEqual(value1, receiver4.Receive());
            Assert.AreEqual(value1, receiver5.Receive());
            Assert.AreEqual(value1, receiver6.Receive());
            Assert.AreEqual(value1, receiver7.Receive());
            Assert.AreEqual(value1, receiver8.Receive());
            Assert.AreEqual(value1, receiver9.Receive());

            sender.Send(value2);
            Assert.AreEqual(value2, receiver1.Receive());
            Assert.AreEqual(value2, receiver2.Receive());
            Assert.AreEqual(value2, receiver3.Receive());
            Assert.AreEqual(value2, receiver4.Receive());
            Assert.AreEqual(value2, receiver5.Receive());
            Assert.AreEqual(value2, receiver6.Receive());
            Assert.AreEqual(value2, receiver7.Receive());
            Assert.AreEqual(value2, receiver8.Receive());
            Assert.AreEqual(value2, receiver9.Receive());

            sender.Send(value3);
            Assert.AreEqual(value3, receiver1.Receive());
            Assert.AreEqual(value3, receiver2.Receive());
            Assert.AreEqual(value3, receiver3.Receive());
            Assert.AreEqual(value3, receiver4.Receive());
            Assert.AreEqual(value3, receiver5.Receive());
            Assert.AreEqual(value3, receiver6.Receive());
            Assert.AreEqual(value3, receiver7.Receive());
            Assert.AreEqual(value3, receiver8.Receive());
            Assert.AreEqual(value3, receiver9.Receive());
        }


        /// <summary>
        /// Tests reduce and broadcast together
        /// </summary>
        [TestMethod]
        public void TestWritableBroadcastReduceOperators()
        {
            string groupName = "group1";
            string broadcastOperatorName = "broadcast";
            string reduceOperatorName = "reduce";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 10;
            int fanOut = 3;

            var groupCommDriver = GetInstanceOfGroupCommDriver(driverId, masterTaskId, groupName, fanOut, numTasks);

            ICommunicationGroupDriver commGroup = groupCommDriver.DefaultGroup
                .AddBroadcast<int>(
                    broadcastOperatorName,
                    masterTaskId,
                    TopologyTypes.Tree,
                    GetDefaulDataConverterConfig())
                .AddReduce<int>(
                    reduceOperatorName,
                    masterTaskId,
                    TopologyTypes.Tree,
                    GetDefaulDataConverterConfig(),
                    GetDefaulReduceFuncConfig())
                .Build();

            var commGroups = CommGroupClients(groupName, numTasks, groupCommDriver, commGroup);

            //for master task
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
                Assert.AreEqual(i, n1);
                Assert.AreEqual(i, n2);
                Assert.AreEqual(i, n3);
                Assert.AreEqual(i, n4);
                Assert.AreEqual(i, n5);
                Assert.AreEqual(i, n6);
                Assert.AreEqual(i, n7);
                Assert.AreEqual(i, n8);
                Assert.AreEqual(i, n9);

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
                Assert.AreEqual(sum, expected);
            }
        }

        private static IWritableGroupCommDriver GetInstanceOfGroupCommDriver(string driverId, string masterTaskId, string groupName, int fanOut, int numTasks)
        {
            var c = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<GroupCommConfigurationOptions.DriverId>(driverId)
                .BindStringNamedParam<GroupCommConfigurationOptions.MasterTaskId>(masterTaskId)
                .BindStringNamedParam<GroupCommConfigurationOptions.GroupName>(groupName)
                .BindIntNamedParam<GroupCommConfigurationOptions.FanOut>(fanOut.ToString())
                .BindIntNamedParam<GroupCommConfigurationOptions.NumberOfTasks>(numTasks.ToString())
                .BindImplementation(GenericType<IConfigurationSerializer>.Class, GenericType<AvroConfigurationSerializer>.Class)
                .Build();

            IWritableGroupCommDriver groupCommDriver = TangFactory.GetTang().NewInjector(c).GetInstance<WritableGroupCommDriver>();
            return groupCommDriver;
        }

        private static List<ICommunicationGroupClient> CommGroupClients(string groupName, int numTasks, IWritableGroupCommDriver groupCommDriver, ICommunicationGroupDriver commGroup)
        {
            List<ICommunicationGroupClient> commGroups = new List<ICommunicationGroupClient>();
            IConfiguration serviceConfig = groupCommDriver.GetServiceConfiguration(GetDefaultCodecConfig());

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
                //get task configuration at driver side
                string taskId = "task" + i;
                IConfiguration groupCommTaskConfig = groupCommDriver.GetGroupCommTaskConfiguration(taskId);
                IConfiguration mergedConf = Configurations.Merge(groupCommTaskConfig, partialConfigs[i], serviceConfig);

                var conf = TangFactory.GetTang()
                    .NewConfigurationBuilder(mergedConf)
                    .BindNamedParameter(typeof(GroupCommConfigurationOptions.Initialize), "false")
                    .Build();
                IInjector injector = TangFactory.GetTang().NewInjector(conf);

                //simulate injection at evaluator side
                IGroupCommClient groupCommClient = injector.GetInstance<WritableGroupCommClient>();
                commGroups.Add(groupCommClient.GetCommunicationGroup(groupName));
            }
            return commGroups;
        }


        private static IConfiguration GetDefaultCodecConfig()
        {
            return StreamingCodecConfiguration<int>.Conf
                .Set(StreamingCodecConfiguration<int>.Codec, GenericType<IntStreamingCodec>.Class)
                .Build();
        }

        private IConfiguration GetDefaulReduceFuncConfig()
        {
            return ReduceFunctionConfiguration<int>.Conf
                .Set(ReduceFunctionConfiguration<int>.ReduceFunction, GenericType<SumFunction>.Class)
                .Build();
        }

        private IConfiguration GetDefaulDataConverterConfig()
        {
            return PipelineDataConverterConfiguration<int>.Conf
                .Set(PipelineDataConverterConfiguration<int>.DataConverter,
                    GenericType<DefaultPipelineDataConverter<int>>.Class)
                .Build();
        }

    }
}

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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Network.Examples.GroupCommunication.BroadcastReduceDriverAndTasks
{
    public class BroadcastReduceDriver : 
        IObserver<IAllocatedEvaluator>, 
        IObserver<IActiveContext>, 
        IObserver<IFailedEvaluator>,
        IObserver<IDriverStarted>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(BroadcastReduceDriver));

        private readonly int _numEvaluators;
        private readonly int _numIterations;
        private readonly IGroupCommDriver _groupCommDriver;
        private ICommunicationGroupDriver _commGroup;
        private TaskStarter _groupCommTaskStarter;
        private readonly IConfiguration _tcpPortProviderConfig;
        private readonly IConfiguration _codecConfig;
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private readonly ContextManager _contextManager;
            
        [Inject]
        public BroadcastReduceDriver(
            [Parameter(typeof(GroupTestConfig.NumEvaluators))] int numEvaluators,
            [Parameter(typeof(GroupTestConfig.NumIterations))] int numIterations,
            [Parameter(typeof(GroupTestConfig.StartingPort))] int startingPort,
            [Parameter(typeof(GroupTestConfig.PortRange))] int portRange,
            GroupCommDriver groupCommDriver,
            IEvaluatorRequestor evaluatorRequestor)
        {
            _numEvaluators = numEvaluators;
            _numIterations = numIterations;
            _groupCommDriver = groupCommDriver;
            _evaluatorRequestor = evaluatorRequestor;

            _contextManager = new ContextManager(_numEvaluators);

            _tcpPortProviderConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<TcpPortRangeStart, int>(GenericType<TcpPortRangeStart>.Class,
                    startingPort.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<TcpPortRangeCount, int>(GenericType<TcpPortRangeCount>.Class,
                    portRange.ToString(CultureInfo.InvariantCulture))
                .Build();

            _codecConfig = StreamingCodecConfiguration<int>.Conf
                .Set(StreamingCodecConfiguration<int>.Codec, GenericType<IntStreamingCodec>.Class)
                .Build();
        }

        private void CreateCommGroup()
        {
            IConfiguration reduceFunctionConfig = ReduceFunctionConfiguration<int>.Conf
                .Set(ReduceFunctionConfiguration<int>.ReduceFunction, GenericType<SumFunction>.Class)
                .Build();

            IConfiguration dataConverterConfig = PipelineDataConverterConfiguration<int>.Conf
                .Set(PipelineDataConverterConfiguration<int>.DataConverter, GenericType<DefaultPipelineDataConverter<int>>.Class)
                .Build();

            _commGroup = _groupCommDriver
                .NewCommunicationGroup(GroupTestConstants.CommGroupName, _numEvaluators)
                .AddBroadcast<int>(
                    GroupTestConstants.BroadcastOperatorName,
                    GroupTestConstants.MasterTaskId,
                    TopologyTypes.Tree,
                    dataConverterConfig)
                .AddReduce<int>(
                    GroupTestConstants.ReduceOperatorName,
                    GroupTestConstants.MasterTaskId,
                    TopologyTypes.Tree,
                    reduceFunctionConfig,
                    dataConverterConfig)
                .Build();
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            IConfiguration contextConf = _groupCommDriver.GetContextConfiguration();
            IConfiguration serviceConf = _groupCommDriver.GetServiceConfiguration();
            serviceConf = Configurations.Merge(serviceConf, _tcpPortProviderConfig, _codecConfig);
            allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
        }

        public void OnNext(IActiveContext value)
        {
            if (_contextManager.AddContext(value))
            {
                SubmitTasks();
            }
        }

        private void SubmitTasks()
        {
            CreateCommGroup();
            _groupCommTaskStarter = new TaskStarter(_groupCommDriver, _numEvaluators);

            foreach (var activeContext in _contextManager.ActiveContexts)
            {
                if (_groupCommDriver.IsMasterTaskContext(activeContext))
                {
                    // Configure Master Task
                    IConfiguration partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                        TaskConfiguration.ConfigurationModule
                            .Set(TaskConfiguration.Identifier, GroupTestConstants.MasterTaskId)
                            .Set(TaskConfiguration.Task, GenericType<MasterTask>.Class)
                            .Build())
                        .BindNamedParameter<GroupTestConfig.NumEvaluators, int>(
                            GenericType<GroupTestConfig.NumEvaluators>.Class,
                            _numEvaluators.ToString(CultureInfo.InvariantCulture))
                        .BindNamedParameter<GroupTestConfig.NumIterations, int>(
                            GenericType<GroupTestConfig.NumIterations>.Class,
                            _numIterations.ToString(CultureInfo.InvariantCulture))
                        .Build();

                    _commGroup.AddTask(GroupTestConstants.MasterTaskId);
                    _groupCommTaskStarter.QueueTask(partialTaskConf, activeContext);
                }
                else
                {
                    // Configure Slave Task
                    string slaveTaskId = "SlaveTask-" + activeContext.Id;
                    IConfiguration partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                        TaskConfiguration.ConfigurationModule
                            .Set(TaskConfiguration.Identifier, slaveTaskId)
                            .Set(TaskConfiguration.Task, GenericType<SlaveTask>.Class)
                            .Build())
                        .BindNamedParameter<GroupTestConfig.NumEvaluators, int>(
                            GenericType<GroupTestConfig.NumEvaluators>.Class,
                            _numEvaluators.ToString(CultureInfo.InvariantCulture))
                        .BindNamedParameter<GroupTestConfig.NumIterations, int>(
                            GenericType<GroupTestConfig.NumIterations>.Class,
                            _numIterations.ToString(CultureInfo.InvariantCulture))
                        .Build();

                    _commGroup.AddTask(slaveTaskId);
                    _groupCommTaskStarter.QueueTask(partialTaskConf, activeContext);
                }
            }
        }

        public void OnNext(IFailedEvaluator value)
        {
            Logger.Log(Level.Error, "In IFailedEvaluator:" + value.Id);
        }

        public void OnNext(IDriverStarted value)
        {
            var request =
                _evaluatorRequestor.NewBuilder()
                    .SetNumber(_numEvaluators)
                    .SetMegabytes(512)
                    .SetCores(2)
                    .SetRackName("WonderlandRack")
                    .SetEvaluatorBatchId("BroadcastEvaluator")
                    .Build();
            _evaluatorRequestor.Submit(request);
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }

        private class SumFunction : IReduceFunction<int>
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
    }
}

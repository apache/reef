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
using System.Linq;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Network.Examples.GroupCommunication.BroadcastReduceDriverAndTasks;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;

namespace Org.Apache.REEF.Network.Examples.GroupCommunication.PipelineBroadcastReduceDriverAndTasks
{
    public class PipelinedBroadcastReduceDriver : 
        IObserver<IAllocatedEvaluator>, 
        IObserver<IActiveContext>, 
        IObserver<IFailedEvaluator>, 
        IObserver<IDriverStarted>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PipelinedBroadcastReduceDriver));
        private readonly int _arraySize;
        private readonly ICommunicationGroupDriver _commGroup;
        private readonly IGroupCommDriver _groupCommDriver;
        private readonly TaskStarter _groupCommTaskStarter;
        private readonly int _numEvaluators;
        private readonly int _numIterations;
        private readonly IConfiguration _tcpPortProviderConfig;
        private readonly IConfiguration _codecConfig;
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        [Inject]
        public PipelinedBroadcastReduceDriver(
            [Parameter(typeof(GroupTestConfig.NumEvaluators))] int numEvaluators,
            [Parameter(typeof(GroupTestConfig.NumIterations))] int numIterations,
            [Parameter(typeof(GroupTestConfig.StartingPort))] int startingPort,
            [Parameter(typeof(GroupTestConfig.PortRange))] int portRange,
            [Parameter(typeof(GroupTestConfig.ChunkSize))] int chunkSize,
            [Parameter(typeof(GroupTestConfig.ArraySize))] int arraySize,
            GroupCommDriver groupCommDriver,
            IEvaluatorRequestor evaluatorRequestor)
        {
            Logger.Log(Level.Info, "entering the driver code " + chunkSize);

            _numEvaluators = numEvaluators;
            _numIterations = numIterations;
            _arraySize = arraySize;
            _evaluatorRequestor = evaluatorRequestor;

            _tcpPortProviderConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<TcpPortRangeStart, int>(GenericType<TcpPortRangeStart>.Class,
                    startingPort.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<TcpPortRangeCount, int>(GenericType<TcpPortRangeCount>.Class,
                    portRange.ToString(CultureInfo.InvariantCulture))
                .Build();

            _codecConfig = StreamingCodecConfiguration<int[]>.Conf
                .Set(StreamingCodecConfiguration<int[]>.Codec, GenericType<IntArrayStreamingCodec>.Class)
                .Build();

            var reduceFunctionConfig = ReduceFunctionConfiguration<int[]>.Conf
                .Set(ReduceFunctionConfiguration<int[]>.ReduceFunction, GenericType<ArraySumFunction>.Class)
                .Build();

            var dataConverterConfig = TangFactory.GetTang().NewConfigurationBuilder(
                PipelineDataConverterConfiguration<int[]>.Conf
                    .Set(PipelineDataConverterConfiguration<int[]>.DataConverter,
                        GenericType<PipelineIntDataConverter>.Class)
                    .Build())
                .BindNamedParameter<GroupTestConfig.ChunkSize, int>(
                    GenericType<GroupTestConfig.ChunkSize>.Class,
                    chunkSize.ToString(CultureInfo.InvariantCulture))
                .Build();

            _groupCommDriver = groupCommDriver;

            _commGroup = _groupCommDriver.DefaultGroup
                .AddBroadcast<int[]>(
                    GroupTestConstants.BroadcastOperatorName,
                    GroupTestConstants.MasterTaskId,
                    TopologyTypes.Tree,
                    dataConverterConfig)
                .AddReduce<int[]>(
                    GroupTestConstants.ReduceOperatorName,
                    GroupTestConstants.MasterTaskId,
                    TopologyTypes.Tree,
                    reduceFunctionConfig,
                    dataConverterConfig)
                .Build();

            _groupCommTaskStarter = new TaskStarter(_groupCommDriver, numEvaluators);
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            IConfiguration contextConf = _groupCommDriver.GetContextConfiguration();
            IConfiguration serviceConf = _groupCommDriver.GetServiceConfiguration();
            serviceConf = Configurations.Merge(serviceConf, _codecConfig, _tcpPortProviderConfig);
            allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
        }
        public void OnNext(IActiveContext activeContext)
        {
            if (_groupCommDriver.IsMasterTaskContext(activeContext))
            {
                Logger.Log(Level.Info, "******* Master ID " + activeContext.Id);

                // Configure Master Task
                var partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, GroupTestConstants.MasterTaskId)
                        .Set(TaskConfiguration.Task, GenericType<PipelinedMasterTask>.Class)
                        .Build())
                    .BindNamedParameter<GroupTestConfig.NumEvaluators, int>(
                        GenericType<GroupTestConfig.NumEvaluators>.Class,
                        _numEvaluators.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter<GroupTestConfig.NumIterations, int>(
                        GenericType<GroupTestConfig.NumIterations>.Class,
                        _numIterations.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter<GroupTestConfig.ArraySize, int>(
                        GenericType<GroupTestConfig.ArraySize>.Class,
                        _arraySize.ToString(CultureInfo.InvariantCulture))
                    .Build();

                _commGroup.AddTask(GroupTestConstants.MasterTaskId);
                _groupCommTaskStarter.QueueTask(partialTaskConf, activeContext);
            }
            else
            {
                // Configure Slave Task
                var slaveTaskId = "SlaveTask-" + activeContext.Id;
                var partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, slaveTaskId)
                        .Set(TaskConfiguration.Task, GenericType<PipelinedSlaveTask>.Class)
                        .Build())
                    .BindNamedParameter<GroupTestConfig.NumEvaluators, int>(
                        GenericType<GroupTestConfig.NumEvaluators>.Class,
                        _numEvaluators.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter<GroupTestConfig.NumIterations, int>(
                        GenericType<GroupTestConfig.NumIterations>.Class,
                        _numIterations.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter<GroupTestConfig.ArraySize, int>(
                        GenericType<GroupTestConfig.ArraySize>.Class,
                        _arraySize.ToString(CultureInfo.InvariantCulture))
                    .Build();

                _commGroup.AddTask(slaveTaskId);
                _groupCommTaskStarter.QueueTask(partialTaskConf, activeContext);
            }
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }

        public void OnNext(IFailedEvaluator value)
        {
        }

        public void OnNext(IDriverStarted value)
        {
            var request =
                _evaluatorRequestor.NewBuilder()
                    .SetNumber(_numEvaluators)
                    .SetMegabytes(512)
                    .SetRackName("WonderlandRack")
                    .SetEvaluatorBatchId("BroadcastEvaluator")
                    .Build();
            _evaluatorRequestor.Submit(request);
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

        private class ArraySumFunction : IReduceFunction<int[]>
        {
            [Inject]
            public ArraySumFunction()
            {
            }

            public int[] Reduce(IEnumerable<int[]> elements)
            {
                int[] result = null;
                var count = 0;

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

                        for (var i = 0; i < result.Length; i++)
                        {
                            result[i] += element[i];
                        }
                    }

                    count++;
                }

                return result;
            }
        }

        private class IntArrayCodec : ICodec<int[]>
        {
            [Inject]
            public IntArrayCodec()
            {
            }

            public byte[] Encode(int[] obj)
            {
                var result = new byte[sizeof(int) * obj.Length];
                Buffer.BlockCopy(obj, 0, result, 0, result.Length);
                return result;
            }

            public int[] Decode(byte[] data)
            {
                if (data.Length % sizeof(int) != 0)
                {
                    throw new Exception(
                        "error inside integer array decoder, byte array length not a multiple of integer size");
                }

                var result = new int[data.Length / sizeof(int)];
                Buffer.BlockCopy(data, 0, result, 0, data.Length);
                return result;
            }
        }

        public class PipelineIntDataConverter : IPipelineDataConverter<int[]>
        {
            private readonly int _chunkSize;

            [Inject]
            public PipelineIntDataConverter([Parameter(typeof(GroupTestConfig.ChunkSize))] int chunkSize)
            {
                _chunkSize = chunkSize;
            }

            public List<PipelineMessage<int[]>> PipelineMessage(int[] message)
            {
                var messageList = new List<PipelineMessage<int[]>>();
                var totalChunks = message.Length / _chunkSize;

                if (message.Length % _chunkSize != 0)
                {
                    totalChunks++;
                }

                var counter = 0;
                for (var i = 0; i < message.Length; i += _chunkSize)
                {
                    var data = new int[Math.Min(_chunkSize, message.Length - i)];
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
                var size = pipelineMessage.Select(x => x.Data.Length).Sum();
                var data = new int[size];
                var offset = 0;

                foreach (var message in pipelineMessage)
                {
                    Buffer.BlockCopy(message.Data, 0, data, offset, message.Data.Length * sizeof(int));
                    offset += message.Data.Length * sizeof(int);
                }

                return data;
            }
        }
    }
}
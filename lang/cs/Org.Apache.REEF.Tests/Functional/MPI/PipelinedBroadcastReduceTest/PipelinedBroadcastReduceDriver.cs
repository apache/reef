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
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Network.Group.Topology;

namespace Org.Apache.REEF.Tests.Functional.MPI.PipelinedBroadcastReduceTest
{
    public class PipelinedBroadcastReduceDriver : IStartHandler, IObserver<IEvaluatorRequestor>, IObserver<IAllocatedEvaluator>, IObserver<IActiveContext>, IObserver<IFailedEvaluator>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PipelinedBroadcastReduceDriver));

        private readonly int _numEvaluators;
        private readonly int _numIterations;
        private readonly int _chunkSize;

        private readonly IMpiDriver _mpiDriver;
        private readonly ICommunicationGroupDriver _commGroup;
        private readonly TaskStarter _mpiTaskStarter;

        [Inject]
        public PipelinedBroadcastReduceDriver(
            [Parameter(typeof (MpiTestConfig.NumEvaluators))] int numEvaluators,
            [Parameter(typeof (MpiTestConfig.NumIterations))] int numIterations,
            [Parameter(typeof (MpiTestConfig.ChunkSize))] int chunkSize,
            MpiDriver mpiDriver)
        {
            Logger.Log(Level.Info, "*******entering the driver code " + chunkSize);

            Identifier = "BroadcastStartHandler";
            _numEvaluators = numEvaluators;
            _numIterations = numIterations;
            _chunkSize = chunkSize;

            _mpiDriver = mpiDriver;

            _commGroup = _mpiDriver.DefaultGroup
                .AddBroadcast<int[], IntArrayCodec>(
                    MpiTestConstants.BroadcastOperatorName,
                    MpiTestConstants.MasterTaskId,
                    TopologyTypes.Tree,
                    new PipelineIntDataConverter(_chunkSize))
                .AddReduce<int[], IntArrayCodec>(
                    MpiTestConstants.ReduceOperatorName,
                    MpiTestConstants.MasterTaskId,
                    new ArraySumFunction(),
                    TopologyTypes.Tree,
                    new PipelineIntDataConverter(_chunkSize))
                .Build();

            _mpiTaskStarter = new TaskStarter(_mpiDriver, numEvaluators);

            CreateClassHierarchy();
        }

        public string Identifier { get; set; }

        public void OnNext(IEvaluatorRequestor evaluatorRequestor)
        {
            EvaluatorRequest request = new EvaluatorRequest(_numEvaluators, 512, 2, "WonderlandRack", "BroadcastEvaluator");
            evaluatorRequestor.Submit(request);
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            IConfiguration contextConf = _mpiDriver.GetContextConfiguration();
            IConfiguration serviceConf = _mpiDriver.GetServiceConfiguration();
            allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
        }

        public void OnNext(IActiveContext activeContext)
        {
            if (_mpiDriver.IsMasterTaskContext(activeContext))
            {
                Logger.Log(Level.Info, "******* Master ID " + activeContext.Id );

                // Configure Master Task
                IConfiguration partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, MpiTestConstants.MasterTaskId)
                        .Set(TaskConfiguration.Task, GenericType<PipelinedMasterTask>.Class)
                        .Build())
                    .BindNamedParameter<MpiTestConfig.NumEvaluators, int>(
                        GenericType<MpiTestConfig.NumEvaluators>.Class,
                        _numEvaluators.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter<MpiTestConfig.NumIterations, int>(
                        GenericType<MpiTestConfig.NumIterations>.Class,
                        _numIterations.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter<MpiTestConfig.ArraySize, int>(
                        GenericType<MpiTestConfig.ArraySize>.Class,
                        MpiTestConstants.ArrayLength.ToString(CultureInfo.InvariantCulture))
                    .Build();

                _commGroup.AddTask(MpiTestConstants.MasterTaskId);
                _mpiTaskStarter.QueueTask(partialTaskConf, activeContext);
            }
            else
            {
                // Configure Slave Task
                string slaveTaskId = "SlaveTask-" + activeContext.Id;
                IConfiguration partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, slaveTaskId)
                        .Set(TaskConfiguration.Task, GenericType<PipelinedSlaveTask>.Class)
                        .Build())
                    .BindNamedParameter<MpiTestConfig.NumEvaluators, int>(
                        GenericType<MpiTestConfig.NumEvaluators>.Class,
                        _numEvaluators.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter<MpiTestConfig.NumIterations, int>(
                        GenericType<MpiTestConfig.NumIterations>.Class,
                        _numIterations.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter<MpiTestConfig.ArraySize, int>(
                        GenericType<MpiTestConfig.ArraySize>.Class,
                        MpiTestConstants.ArrayLength.ToString(CultureInfo.InvariantCulture))
                    .Build();

                _commGroup.AddTask(slaveTaskId);
                _mpiTaskStarter.QueueTask(partialTaskConf, activeContext);
            }
        }

        public void OnNext(IFailedEvaluator value)
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        private void CreateClassHierarchy()
        {
            HashSet<string> clrDlls = new HashSet<string>();
            clrDlls.Add(typeof(IDriver).Assembly.GetName().Name);
            clrDlls.Add(typeof(ITask).Assembly.GetName().Name);
            clrDlls.Add(typeof(PipelinedBroadcastReduceDriver).Assembly.GetName().Name);
            clrDlls.Add(typeof(INameClient).Assembly.GetName().Name);
            clrDlls.Add(typeof(INetworkService<>).Assembly.GetName().Name);

            ClrHandlerHelper.GenerateClassHierarchy(clrDlls);
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


        private class IntArrayCodec : ICodec<int[]>
        {
            [Inject]
            public IntArrayCodec()
            {
            }

            public byte[] Encode(int[] obj)
            {
                byte[] result = new byte[sizeof(Int32) * obj.Length];
                Buffer.BlockCopy(obj, 0, result, 0, result.Length);
                return result;
            }

            public int[] Decode(byte[] data)
            {
                if (data.Length % sizeof(Int32) != 0)
                {
                    throw new Exception("error inside integer array decoder, byte array length not a multiple of interger size");
                }

                int[] result = new int[data.Length / sizeof(Int32)];
                Buffer.BlockCopy(data, 0, result, 0, data.Length);
                return result;
            }
        }

        public class PipelineIntDataConverter : IPipelineDataConverter<int[]>
        {
            readonly int _chunkSize;
            
            [Inject]
            public PipelineIntDataConverter([Parameter(typeof(MpiTestConfig.ChunkSize))] int chunkSize)
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

            public IConfiguration GetConfiguration()
            {
                return TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<MpiTestConfig.ChunkSize, int>(GenericType<MpiTestConfig.ChunkSize>.Class, _chunkSize.ToString(CultureInfo.InvariantCulture))
                .Build();
            }
        }
    }
}
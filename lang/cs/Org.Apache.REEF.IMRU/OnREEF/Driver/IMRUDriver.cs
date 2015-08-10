﻿// Licensed to the Apache Software Foundation (ASF) under one
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
using System.Globalization;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Implements the IMRU driver on REEF
    /// </summary>
    /// <typeparam name="TMapInput">Map Input</typeparam>
    /// <typeparam name="TMapOutput">Map output</typeparam>
    /// <typeparam name="TResult">Result</typeparam>
    internal sealed class IMRUDriver<TMapInput, TMapOutput, TResult> : IObserver<IDriverStarted>,
        IObserver<IAllocatedEvaluator>, IObserver<IActiveContext>, IObserver<ICompletedTask>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof (IMRUDriver<TMapInput, TMapOutput, TResult>));

        private readonly ConfigurationManager _configurationManager;
        private readonly IPartitionedDataSet _dataSet;
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private ICommunicationGroupDriver _commGroup;
        private readonly IGroupCommDriver _groupCommDriver;
        private readonly TaskStarter _groupCommTaskStarter;
        private IConfiguration _tcpPortProviderConfig;
        private readonly ConcurrentStack<string> _taskIdStack;
        private readonly ConcurrentStack<IPartitionDescriptor> _partitionDescriptorStack;
        private readonly int _coresPerMapper;
        private readonly int _coresForUpdateTask;
        private readonly int _memoryPerMapper;
        private readonly int _memoryForUpdateTask;
        private bool _allocatedUpdateTaskEvaluator;
        private readonly ConcurrentBag<ICompletedTask> _completedTasks;
            
        [Inject]
        private IMRUDriver(IPartitionedDataSet dataSet,
            ConfigurationManager configurationManager,
            IEvaluatorRequestor evaluatorRequestor,
            [Parameter(typeof (TcpPortRangeStart))] int startingPort,
            [Parameter(typeof (TcpPortRangeCount))] int portRange,
            [Parameter(typeof(CoresPerMapper))] int coresPerMapper,
            [Parameter(typeof(CoresForUpdateTask))] int coresForUpdateTask,
            [Parameter(typeof(MemoryPerMapper))] int memoryPerMapper,
            [Parameter(typeof(MemoryForUpdateTask))] int memoryForUpdateTask,
            IGroupCommDriver groupCommDriver)
        {
            _dataSet = dataSet;
            _configurationManager = configurationManager;
            _evaluatorRequestor = evaluatorRequestor;
            _groupCommDriver = groupCommDriver;
            _coresPerMapper = coresPerMapper;
            _coresForUpdateTask = coresForUpdateTask;
            _memoryPerMapper = memoryPerMapper;
            _memoryForUpdateTask = memoryForUpdateTask;
            _allocatedUpdateTaskEvaluator = false;
            _completedTasks = new ConcurrentBag<ICompletedTask>();

            AddGroupCommunicationOperators();
            ConstructTcpPortProviderConfig(startingPort, portRange);

            _groupCommTaskStarter = new TaskStarter(_groupCommDriver, _dataSet.Count + 1);

            _taskIdStack = new ConcurrentStack<string>();
            _partitionDescriptorStack = new ConcurrentStack<IPartitionDescriptor>();
            ConstructTaskIdAndPartitionDescriptorStack();
        }

        /// <summary>
        /// Requests for evaluator for update task
        /// </summary>
        /// <param name="value">Even fired when driver started</param>
        public void OnNext(IDriverStarted value)
        {
            _evaluatorRequestor.Submit(new EvaluatorRequest(1, _memoryForUpdateTask, _coresForUpdateTask));
            //  TODO: Set a timeout for this request to be satisfied. If it is not within that time, exit the Driver.
        }

        /// <summary>
        /// Specifies context and service configuration for evaluator depending
        /// on whether it is for Update function or for map function
        /// </summary>
        /// <param name="allocatedEvaluator">The allocated evaluator</param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            IConfiguration contextConf = _groupCommDriver.GetContextConfiguration();
            IConfiguration serviceConf = _groupCommDriver.GetServiceConfiguration();

            if (!_allocatedUpdateTaskEvaluator)
            {
                var codecConfig =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder(_configurationManager.UpdateFunctionCodecsConfiguration)
                        .BindImplementation(GenericType<IStreamingCodec<MapInputWithControlMessage<TMapInput>>>.Class,
                            GenericType<MapInputWithControlMessageCodec<TMapInput>>.Class)
                        .Build();

                serviceConf = Configurations.Merge(serviceConf, codecConfig, _tcpPortProviderConfig);
                _allocatedUpdateTaskEvaluator = true;

                _evaluatorRequestor.Submit(new EvaluatorRequest(_dataSet.Count, _memoryPerMapper, _coresPerMapper));
            }
            else
            {
                IPartitionDescriptor partitionDescriptor;

                if (!_partitionDescriptorStack.TryPop(out partitionDescriptor))
                {
                    Logger.Log(Level.Warning, "partition descriptor exist for the context of evaluator");
                    allocatedEvaluator.Dispose();
                    return;
                }

                var mapInputWithControlMesageCodecConfig =
                    TangFactory.GetTang().NewConfigurationBuilder(_configurationManager.MapInputCodecConfiguration)
                        .BindImplementation(GenericType<IStreamingCodec<MapInputWithControlMessage<TMapInput>>>.Class,
                            GenericType<MapInputWithControlMessageCodec<TMapInput>>.Class)
                        .Build();

                contextConf = Configurations.Merge(contextConf, partitionDescriptor.GetPartitionConfiguration());
                serviceConf = Configurations.Merge(serviceConf, mapInputWithControlMesageCodecConfig,
                    _tcpPortProviderConfig);
            }
            
            allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
        }

        /// <summary>
        /// Specfies the Map or Update task to run on the active context
        /// </summary>
        /// <param name="activeContext"></param>
        public void OnNext(IActiveContext activeContext)
        {
            Logger.Log(Level.Info, string.Format("Received Active Context {0}", activeContext.Id));

            if (_groupCommDriver.IsMasterTaskContext(activeContext))
            {
                var partialTaskConf =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder(new[]
                        {
                            TaskConfiguration.ConfigurationModule
                                .Set(TaskConfiguration.Identifier,
                                    IMRUConstants.UpdateTaskName)
                                .Set(TaskConfiguration.Task,
                                    GenericType<UpdateTaskHost<TMapInput, TMapOutput, TResult>>.Class)
                                .Build(),
                            _configurationManager.UpdateFunctionConfiguration
                        })
                        .Build();

                _commGroup.AddTask(IMRUConstants.UpdateTaskName);
                _groupCommTaskStarter.QueueTask(partialTaskConf, activeContext);
            }
            else
            {
                string taskId;

                if (!_taskIdStack.TryPop(out taskId))
                {
                    Logger.Log(Level.Warning, "No task Ids exist for the active context {0}. Disposing the context.",
                        activeContext.Id);
                    activeContext.Dispose();
                    return;
                }

                var partialTaskConf =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder(new[]
                        {
                            TaskConfiguration.ConfigurationModule
                                .Set(TaskConfiguration.Identifier, taskId)
                                .Set(TaskConfiguration.Task, GenericType<MapTaskHost<TMapInput, TMapOutput>>.Class)
                                .Build(),
                            _configurationManager.MapFunctionConfiguration
                        })
                        .Build();

                _commGroup.AddTask(taskId);
                _groupCommTaskStarter.QueueTask(partialTaskConf, activeContext);
            }
        }

        /// <summary>
        /// Specfies what to do when the task is completed
        /// In this case just disposes off the task
        /// </summary>
        /// <param name="completedTask">The link to the completed task</param>
        public void OnNext(ICompletedTask completedTask)
        {
            _completedTasks.Add(completedTask);

            if (_completedTasks.Count != _dataSet.Count + 1) return;
            
            foreach (var task in _completedTasks)
            {
                task.ActiveContext.Dispose();
            }
        }

        /// <summary>
        /// Specfies how to handle exception or error
        /// </summary>
        /// <param name="error">Kind of exception</param>
        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Specfies what to do when driver is done
        /// In this case do nothing
        /// </summary>
        public void OnCompleted()
        {
        }

        private void AddGroupCommunicationOperators()
        {
            var reduceFunctionConfig = _configurationManager.ReduceFunctionConfiguration;
            var mapOutputPipelineDataConverterConfig = _configurationManager.MapOutputPipelineDataConverterConfiguration;
            var mapInputPipelineDataConverterConfig = _configurationManager.MapInputPipelineDataConverterConfiguration;

            mapInputPipelineDataConverterConfig =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(mapInputPipelineDataConverterConfig)
                    .BindImplementation(
                        GenericType<IPipelineDataConverter<MapInputWithControlMessage<TMapInput>>>.Class,
                        GenericType<MapInputwithControlMessagePipelineDataConverter<TMapInput>>.Class)
                    .Build();

            _commGroup =
                _groupCommDriver.NewCommunicationGroup(IMRUConstants.CommunicationGroupName, _dataSet.Count + 1)
                    .AddBroadcast<MapInputWithControlMessage<TMapInput>>(
                        IMRUConstants.BroadcastOperatorName,
                        IMRUConstants.UpdateTaskName,
                        TopologyTypes.Tree,
                        mapInputPipelineDataConverterConfig)
                    .AddReduce<TMapOutput>(
                        IMRUConstants.ReduceOperatorName,
                        IMRUConstants.UpdateTaskName,
                        TopologyTypes.Tree,
                        reduceFunctionConfig,
                        mapOutputPipelineDataConverterConfig)
                    .Build();
        }

        private void ConstructTcpPortProviderConfig(int startingPort, int portRange)
        {
            _tcpPortProviderConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<TcpPortRangeStart, int>(GenericType<TcpPortRangeStart>.Class,
                    startingPort.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<TcpPortRangeCount, int>(GenericType<TcpPortRangeCount>.Class,
                    portRange.ToString(CultureInfo.InvariantCulture))
                .Build();
        }

        private void ConstructTaskIdAndPartitionDescriptorStack()
        {
            int counter = 0;

            foreach (var partitionDescriptor in _dataSet)
            {
                string id = IMRUConstants.MapTaskPrefix + "-Id" + counter + "-Version0";
                _taskIdStack.Push(id);
                _partitionDescriptorStack.Push(partitionDescriptor);
                counter++;
            }
        }
    }
}
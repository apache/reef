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
using System.Linq;
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
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Parameters;

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
        private readonly ConcurrentStack<IConfiguration> _perMapperConfiguration;
        private readonly ConcurrentStack<IPartitionDescriptor> _partitionDescriptorStack;
        private readonly int _coresPerMapper;
        private readonly int _coresForUpdateTask;
        private readonly int _memoryPerMapper;
        private readonly int _memoryForUpdateTask;
        private readonly ISet<IPerMapperConfigGenerator> _perMapperConfigs;
        private bool _allocatedUpdateTaskEvaluator;
        private readonly ConcurrentBag<ICompletedTask> _completedTasks;
            
        [Inject]
        private IMRUDriver(IPartitionedDataSet dataSet,
            [Parameter(typeof(PerMapConfigGeneratorSet))] ISet<IPerMapperConfigGenerator> perMapperConfigs,
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
            _perMapperConfigs = perMapperConfigs;
            _allocatedUpdateTaskEvaluator = false;
            _completedTasks = new ConcurrentBag<ICompletedTask>();

            AddGroupCommunicationOperators();
            
            //TODO[REEF-600]: Once the configuration module for TcpPortProvider 
            //TODO[REEF-600]: will be provided, the configuraiton will be automatically
            //TODO[REEF-600]: carried over to evaluators and below function will be obsolete.
            ConstructTcpPortProviderConfig(startingPort, portRange);

            _groupCommTaskStarter = new TaskStarter(_groupCommDriver, _dataSet.Count + 1);

            _taskIdStack = new ConcurrentStack<string>();
            _perMapperConfiguration = new ConcurrentStack<IConfiguration>();
            _partitionDescriptorStack = new ConcurrentStack<IPartitionDescriptor>();
            ConstructTaskIdAndPartitionDescriptorStack();
        }

        /// <summary>
        /// Requests for evaluator for update task
        /// </summary>
        /// <param name="value">Event fired when driver started</param>
        public void OnNext(IDriverStarted value)
        {
            var request =
                _evaluatorRequestor.NewBuilder()
                    .SetCores(_coresForUpdateTask)
                    .SetMegabytes(_memoryForUpdateTask)
                    .SetNumber(1)
                    .Build();
            _evaluatorRequestor.Submit(request);
            //TODO[REEF-598]: Set a timeout for this request to be satisfied. If it is not within that time, exit the Driver.
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
                        .NewConfigurationBuilder(
                            new[]
                            {
                                StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Conf.Set(
                                    StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Codec,
                                    GenericType<MapInputWithControlMessageCodec<TMapInput>>.Class).Build(),
                                StreamingCodecConfigurationMinusMessage<TMapOutput>.Conf.Build(),
                                _configurationManager.UpdateFunctionCodecsConfiguration
                            }
                        ).Build();
               
                serviceConf = Configurations.Merge(serviceConf, codecConfig, _tcpPortProviderConfig);
                _allocatedUpdateTaskEvaluator = true;

                var request =
                    _evaluatorRequestor.NewBuilder()
                        .SetMegabytes(_memoryForUpdateTask)
                        .SetNumber(_dataSet.Count)
                        .SetCores(_coresPerMapper)
                        .Build();
                _evaluatorRequestor.Submit(request);
                //TODO[REEF-598]: Set a timeout for this request to be satisfied. If it is not within that time, exit the Driver.
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

                var codecConfig =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder(
                            new[]
                            {
                                StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Conf.Set(
                                    StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Codec,
                                    GenericType<MapInputWithControlMessageCodec<TMapInput>>.Class).Build(),
                                StreamingCodecConfigurationMinusMessage<TMapOutput>.Conf.Build(),
                                _configurationManager.MapInputCodecConfiguration
                            }
                        ).Build();

                contextConf = Configurations.Merge(contextConf, partitionDescriptor.GetPartitionConfiguration());
                serviceConf = Configurations.Merge(serviceConf, codecConfig,
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
            Logger.Log(Level.Verbose, string.Format("Received Active Context {0}", activeContext.Id));

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

                IConfiguration mapSpecificConfig;

                if (!_perMapperConfiguration.TryPop(out mapSpecificConfig))
                {
                    Logger.Log(Level.Warning,
                        "No per map configuration exist for the active context {0}. Disposing the context.",
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
                            _configurationManager.MapFunctionConfiguration,
                            mapSpecificConfig
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
                Logger.Log(Level.Verbose, String.Format("Disposing task: {0}", task.Id));
                task.ActiveContext.Dispose();
            }
        }

        /// <summary>
        /// Specfies how to handle exception or error
        /// </summary>
        /// <param name="error">Kind of exception</param>
        public void OnError(Exception error)
        {
            Logger.Log(Level.Error,"Cannot currently handle the Exception in OnError function");
            throw new NotImplementedException("Cannot currently handle exception in OneError", error);
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

            try
            {
                TangFactory.GetTang()
                    .NewInjector(mapInputPipelineDataConverterConfig)
                    .GetInstance<IPipelineDataConverter<TMapInput>>();

                mapInputPipelineDataConverterConfig =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder(mapInputPipelineDataConverterConfig)
                        .BindImplementation(
                            GenericType<IPipelineDataConverter<MapInputWithControlMessage<TMapInput>>>.Class,
                            GenericType<MapInputwithControlMessagePipelineDataConverter<TMapInput>>.Class)
                        .Build();
            }
            catch (Exception)
            {
                mapInputPipelineDataConverterConfig = TangFactory.GetTang()
                    .NewConfigurationBuilder()
                    .BindImplementation(
                        GenericType<IPipelineDataConverter<MapInputWithControlMessage<TMapInput>>>.Class,
                        GenericType<DefaultPipelineDataConverter<MapInputWithControlMessage<TMapInput>>>.Class)
                    .Build();
            }

            try
            {
                TangFactory.GetTang()
                    .NewInjector(mapInputPipelineDataConverterConfig)
                    .GetInstance<IPipelineDataConverter<TMapOutput>>();
            }
            catch (Exception)
            {
                mapOutputPipelineDataConverterConfig =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder()
                        .BindImplementation(GenericType<IPipelineDataConverter<TMapOutput>>.Class,
                            GenericType<DefaultPipelineDataConverter<TMapOutput>>.Class)
                        .Build();
            }

            _commGroup =
                _groupCommDriver.DefaultGroup
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

                var emptyConfig = TangFactory.GetTang().NewConfigurationBuilder().Build();
                IConfiguration config = _perMapperConfigs.Aggregate(emptyConfig, (current, configGenerator) => Configurations.Merge(current, configGenerator.GetMapperConfiguration(counter, _dataSet.Count)));
                _perMapperConfiguration.Push(config);
                counter++;
            }
        }
    }
}
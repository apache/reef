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
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.IMRU.OnREEF.ResultHandler;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Implements the IMRU driver on REEF
    /// </summary>
    /// <typeparam name="TMapInput">Map Input</typeparam>
    /// <typeparam name="TMapOutput">Map output</typeparam>
    /// <typeparam name="TResult">Result</typeparam>
    internal sealed class IMRUDriver<TMapInput, TMapOutput, TResult> : IObserver<IDriverStarted>,
        IObserver<IAllocatedEvaluator>, IObserver<IActiveContext>, IObserver<ICompletedTask>, IObserver<IFailedEvaluator>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(IMRUDriver<TMapInput, TMapOutput, TResult>));

        private readonly ConfigurationManager _configurationManager;
        private readonly IPartitionedInputDataSet _dataSet;
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private ICommunicationGroupDriver _commGroup;
        private readonly IGroupCommDriver _groupCommDriver;
        private TaskStarter _groupCommTaskStarter;
        private readonly ConcurrentStack<string> _taskIdStack;
        private readonly ConcurrentStack<IConfiguration> _perMapperConfiguration;
        private readonly Stack<IPartitionDescriptor> _partitionDescriptorStack;
        private readonly int _coresPerMapper;
        private readonly int _coresForUpdateTask;
        private readonly int _memoryPerMapper;
        private readonly int _memoryForUpdateTask;
        private readonly ISet<IPerMapperConfigGenerator> _perMapperConfigs;
        private readonly ConcurrentBag<ICompletedTask> _completedTasks;
        private readonly int _allowedFailedEvaluators;
        private int _currentFailedEvaluators = 0;
        private bool _reachedUpdateTaskActiveContext = false;
        private readonly bool _invokeGC;
        private readonly ContextManager _contextManager;

        private readonly ServiceAndContextConfigurationProvider<TMapInput, TMapOutput>
            _serviceAndContextConfigurationProvider;

        [Inject]
        private IMRUDriver(IPartitionedInputDataSet dataSet,
            [Parameter(typeof(PerMapConfigGeneratorSet))] ISet<IPerMapperConfigGenerator> perMapperConfigs,
            ConfigurationManager configurationManager,
            IEvaluatorRequestor evaluatorRequestor,
            [Parameter(typeof(CoresPerMapper))] int coresPerMapper,
            [Parameter(typeof(CoresForUpdateTask))] int coresForUpdateTask,
            [Parameter(typeof(MemoryPerMapper))] int memoryPerMapper,
            [Parameter(typeof(MemoryForUpdateTask))] int memoryForUpdateTask,
            [Parameter(typeof(AllowedFailedEvaluatorsFraction))] double failedEvaluatorsFraction,
            [Parameter(typeof(InvokeGC))] bool invokeGC,
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
            _completedTasks = new ConcurrentBag<ICompletedTask>();
            _allowedFailedEvaluators = (int)(failedEvaluatorsFraction * dataSet.Count);
            _invokeGC = invokeGC;

            _contextManager = new ContextManager(_dataSet.Count + 1);
            _taskIdStack = new ConcurrentStack<string>();
            _perMapperConfiguration = new ConcurrentStack<IConfiguration>();
            _partitionDescriptorStack = new Stack<IPartitionDescriptor>();
            ConstructTaskIdAndPartitionDescriptorStack();
            _serviceAndContextConfigurationProvider =
                new ServiceAndContextConfigurationProvider<TMapInput, TMapOutput>(dataSet.Count + 1, groupCommDriver,
                    _configurationManager, _partitionDescriptorStack);

            var msg = string.Format("map task memory:{0}, update task memory:{1}, map task cores:{2}, update task cores:{3}",
                _memoryPerMapper, _memoryForUpdateTask, _coresPerMapper, _coresForUpdateTask);
            Logger.Log(Level.Info, msg);
        }

        /// <summary>
        /// Requests for evaluator for update task
        /// </summary>
        /// <param name="value">Event fired when driver started</param>
        public void OnNext(IDriverStarted value)
        {
            RequestUpdateEvaluator();
            //// TODO[REEF-598]: Set a timeout for this request to be satisfied. If it is not within that time, exit the Driver.
        }

        /// <summary>
        /// Specifies context and service configuration for evaluator depending
        /// on whether it is for Update function or for map function
        /// Also handles evaluator failures
        /// </summary>
        /// <param name="allocatedEvaluator">The allocated evaluator</param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            var configs = _serviceAndContextConfigurationProvider.GetNextConfiguration(allocatedEvaluator.Id);
            allocatedEvaluator.SubmitContextAndService(configs.Context, configs.Service);
        }

        /// <summary>
        /// Specifies the Map or Update task to run on the active context
        /// </summary>
        /// <param name="activeContext"></param>
        public void OnNext(IActiveContext activeContext)
        {
            Logger.Log(Level.Verbose, string.Format("Received Active Context {0}", activeContext.Id));

            if (_groupCommDriver.IsMasterTaskContext(activeContext))
            {
                _reachedUpdateTaskActiveContext = true;
                RequestMapEvaluators(_dataSet.Count);
            }

            if (_contextManager.AddContext(activeContext))
            {
                SubmitTasks();
            }
        }

        private void SubmitTasks()
        {
            AddGroupCommunicationOperators();
            _groupCommTaskStarter = new TaskStarter(_groupCommDriver, _dataSet.Count + 1);

            //// TODO [REEF-1223]: Evaluator may fail during task preparation and submission. More mechanism will be added when working of it.
            foreach (var activeContext in _contextManager.ActiveContexts)
            {
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
                                _configurationManager.UpdateFunctionConfiguration,
                                _configurationManager.ResultHandlerConfiguration
                            })
                            .BindNamedParameter(typeof(InvokeGC), _invokeGC.ToString())
                            .Build();

                    try
                    {
                        TangFactory.GetTang()
                            .NewInjector(partialTaskConf, _configurationManager.UpdateFunctionCodecsConfiguration)
                            .GetInstance<IIMRUResultHandler<TResult>>();
                    }
                    catch (InjectionException)
                    {
                        partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(partialTaskConf)
                            .BindImplementation(GenericType<IIMRUResultHandler<TResult>>.Class,
                                GenericType<DefaultResultHandler<TResult>>.Class)
                            .Build();
                        Logger.Log(Level.Warning,
                            "User has not given any way to handle IMRU result, defaulting to ignoring it");
                    }

                    _commGroup.AddTask(IMRUConstants.UpdateTaskName);
                    _groupCommTaskStarter.QueueTask(partialTaskConf, activeContext);
                }
                else
                {
                    string taskId;

                    if (!_taskIdStack.TryPop(out taskId))
                    {
                        Logger.Log(Level.Warning,
                            "No task Ids exist for the active context {0}. Disposing the context.",
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
                                    .Set(TaskConfiguration.Task,
                                        GenericType<MapTaskHost<TMapInput, TMapOutput>>.Class)
                                    .Build(),
                                _configurationManager.MapFunctionConfiguration,
                                mapSpecificConfig
                            })
                            .BindNamedParameter(typeof(InvokeGC), _invokeGC.ToString())
                            .Build();

                    _commGroup.AddTask(taskId);
                    _groupCommTaskStarter.QueueTask(partialTaskConf, activeContext);
                }
            }
        }

        /// <summary>
        /// Specifies what to do when the task is completed
        /// In this case just disposes off the task
        /// </summary>
        /// <param name="completedTask">The link to the completed task</param>
        public void OnNext(ICompletedTask completedTask)
        {
            _completedTasks.Add(completedTask);

            if (_completedTasks.Count != _dataSet.Count + 1)
            {
                return;
            }
            
            foreach (var task in _completedTasks)
            {
                Logger.Log(Level.Verbose, string.Format("Disposing task: {0}", task.Id));
                task.ActiveContext.Dispose();
            }
        }

        public void OnNext(IFailedEvaluator value)
        {
            Logger.Log(Level.Info, "An evaluator failed, checking if it failed before context and service was submitted");
            int currFailedEvaluators = Interlocked.Increment(ref _currentFailedEvaluators);

            if (value.FailedContexts != null && value.FailedContexts.Count != 0)
            {
                Logger.Log(Level.Info, "Some active context failed, cannot continue IMRU task");        
                Exceptions.Throw(new Exception(), Logger);
            }

            if (currFailedEvaluators > _allowedFailedEvaluators)
            {
                Exceptions.Throw(new Exception("Cannot continue IMRU job, Failed evaluators reach maximum limit"),
                    Logger);
            }

            Logger.Log(Level.Info, "Requesting for the failed evaluator again");

            _serviceAndContextConfigurationProvider.EvaluatorFailed(value.Id);

            // if active context stage is reached for Update Task then assume that failed
            // evaluator belongs to mapper
            if (_reachedUpdateTaskActiveContext)
            {
                RequestMapEvaluators(1);
            }
            else
            {
                RequestUpdateEvaluator();
            }
        }

        /// <summary>
        /// Specifies how to handle exception or error
        /// </summary>
        /// <param name="error">Kind of exception</param>
        public void OnError(Exception error)
        {
            Logger.Log(Level.Error, "Cannot currently handle the Exception in OnError function");
            throw new NotImplementedException("Cannot currently handle exception in OneError", error);
        }

        /// <summary>
        /// Specifies what to do when driver is done
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
                    .NewInjector(mapOutputPipelineDataConverterConfig)
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

        private void RequestMapEvaluators(int numEvaluators)
        {
            _evaluatorRequestor.Submit(
                _evaluatorRequestor.NewBuilder()
                    .SetMegabytes(_memoryPerMapper)
                    .SetNumber(numEvaluators)
                    .SetCores(_coresPerMapper)
                    .Build());
        }

        private void RequestUpdateEvaluator()
        {
            _evaluatorRequestor.Submit(
                _evaluatorRequestor.NewBuilder()
                    .SetCores(_coresForUpdateTask)
                    .SetMegabytes(_memoryForUpdateTask)
                    .SetNumber(1)
                    .Build());
        }
    }
}
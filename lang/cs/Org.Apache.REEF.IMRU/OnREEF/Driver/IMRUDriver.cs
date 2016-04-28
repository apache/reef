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
using Org.Apache.REEF.Network.Group.Config;
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
    /// <typeparam name="TPartitionType">Type of data partition (Generic type in IInputPartition)</typeparam>
    internal sealed class IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType> 
        : IObserver<IDriverStarted>,
        IObserver<IAllocatedEvaluator>,
        IObserver<IActiveContext>,
        IObserver<ICompletedTask>,
        IObserver<IFailedEvaluator>,
        IObserver<IFailedContext>,
        IObserver<IFailedTask>
    {
        private static readonly Logger Logger =
            Logger.GetLogger(typeof(IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>));

        private readonly ConfigurationManager _configurationManager;
        private readonly int _totalMappers;
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private ICommunicationGroupDriver _commGroup;
        private readonly IGroupCommDriver _groupCommDriver;
        private TaskStarter _groupCommTaskStarter;
        private readonly ConcurrentStack<IConfiguration> _perMapperConfiguration;
        private readonly int _coresPerMapper;
        private readonly int _coresForUpdateTask;
        private readonly int _memoryPerMapper;
        private readonly int _memoryForUpdateTask;
        private readonly ISet<IPerMapperConfigGenerator> _perMapperConfigs;
        private readonly ISet<ICompletedTask> _completedTasks = new HashSet<ICompletedTask>();
        private readonly IDictionary<string, IActiveContext> _activeContexts = new Dictionary<string, IActiveContext>();
        private readonly int _allowedFailedEvaluators;
        private int _currentFailedEvaluators = 0;
        private readonly bool _invokeGC;
        private int _numberOfReadyTasks = 0;
        private readonly object _lock = new object();

        private readonly ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TPartitionType>
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
            _configurationManager = configurationManager;
            _evaluatorRequestor = evaluatorRequestor;
            _groupCommDriver = groupCommDriver;
            _coresPerMapper = coresPerMapper;
            _coresForUpdateTask = coresForUpdateTask;
            _memoryPerMapper = memoryPerMapper;
            _memoryForUpdateTask = memoryForUpdateTask;
            _perMapperConfigs = perMapperConfigs;
            _totalMappers = dataSet.Count;

            _allowedFailedEvaluators = (int)(failedEvaluatorsFraction * dataSet.Count);
            _invokeGC = invokeGC;

            _perMapperConfiguration = ConstructPerMapperConfigStack(_totalMappers);
            _serviceAndContextConfigurationProvider =
                new ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TPartitionType>(dataSet);

            var msg =
                string.Format("map task memory:{0}, update task memory:{1}, map task cores:{2}, update task cores:{3}",
                    _memoryPerMapper,
                    _memoryForUpdateTask,
                    _coresPerMapper,
                    _coresForUpdateTask);
            Logger.Log(Level.Info, msg);
        }

        /// <summary>
        /// Requests for evaluator for update task
        /// </summary>
        /// <param name="value">Event fired when driver started</param>
        public void OnNext(IDriverStarted value)
        {
            RequestUpdateEvaluator();
            RequestMapEvaluators(_totalMappers);
            //// TODO[REEF-598]: Set a timeout for this request to be satisfied. If it is not within that time, exit the Driver.
        }

        /// <summary>
        /// Specifies context and service configuration for evaluator depending
        /// on whether it is for Update function or for map function
        /// </summary>
        /// <param name="allocatedEvaluator">The allocated evaluator</param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            var configs =
                _serviceAndContextConfigurationProvider.GetContextConfigurationForEvaluatorById(allocatedEvaluator.Id);
            allocatedEvaluator.SubmitContextAndService(configs.Context, configs.Service);
        }

        /// <summary>
        /// Adds active context to _activeContexts collection. After all the active context is received, calls SubmitTasks().
        /// </summary>
        /// <param name="activeContext"></param>
        public void OnNext(IActiveContext activeContext)
        {
            Logger.Log(Level.Verbose, string.Format(CultureInfo.InvariantCulture, "Received Active Context {0}", activeContext.Id));

            lock (_lock)
            {
                if (_activeContexts.ContainsKey(activeContext.Id))
                {
                    var msg = string.Format(CultureInfo.InvariantCulture, "The context [{0}] received is already exists.", activeContext.Id);
                    Exceptions.Throw(new ApplicationException(msg), Logger);
                }
                _activeContexts.Add(activeContext.Id, activeContext);

                if (_activeContexts.Count == _totalMappers + 1)
                {
                    SubmitTasks();
                }
            }
        }

        /// <summary>
        /// Creates a new Communication Group and adds Group Communication Operators,
        /// specifies the Map or Update task to run on each active context.
        /// </summary>
        private void SubmitTasks()
        {
            lock (_lock)
            {
                AddGroupCommunicationOperators();
                _groupCommTaskStarter = new TaskStarter(_groupCommDriver, _totalMappers + 1);

                foreach (var activeContext in _activeContexts.Values)
                {
                    if (_serviceAndContextConfigurationProvider.IsMasterEvaluatorId(activeContext.EvaluatorId))
                    {
                        Logger.Log(Level.Verbose, "Submitting master task");
                        _commGroup.AddTask(IMRUConstants.UpdateTaskName);
                        _groupCommTaskStarter.QueueTask(GetUpdateTaskConfiguration(), activeContext);
                    }
                    else
                    {
                        Logger.Log(Level.Verbose, "Submitting map task");
                        _serviceAndContextConfigurationProvider.RecordActiveContextPerEvaluatorId(
                            activeContext.EvaluatorId);
                        string taskId = GetTaskIdByEvaluatorId(activeContext.EvaluatorId);
                        _commGroup.AddTask(taskId);
                        _groupCommTaskStarter.QueueTask(GetMapTaskConfiguration(activeContext, taskId), activeContext);
                        _numberOfReadyTasks++;
                        Logger.Log(Level.Verbose,
                            string.Format("{0} Tasks are ready for submission", _numberOfReadyTasks));
                    }
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
            lock (_completedTasks)
            {
                Logger.Log(Level.Info,
                    string.Format("Received completed task message from task Id: {0}", completedTask.Id));
                _completedTasks.Add(completedTask);

                if (AreIMRUTasksCompleted())
                {
                    ShutDownAllEvaluators();
                }
            }
        }

        /// <summary>
        /// Specifies what to do when evaluator fails.
        /// If we get all completed tasks then ignore the failure
        /// Else request a new evaluator. If failure happens in middle of IMRU 
        /// job we expect neighboring evaluators to fail while doing 
        /// communication and will use FailedTask and FailedContext logic to 
        /// order shutdown.
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(IFailedEvaluator value)
        {
            if (AreIMRUTasksCompleted())
            {
                Logger.Log(Level.Info,
                    string.Format("Evaluator with Id: {0} failed but IMRU task is completed. So ignoring.", value.Id));
                return;
            }

            Logger.Log(Level.Info,
                string.Format("Evaluator with Id: {0} failed with Exception: {1}", value.Id, value.EvaluatorException));
            int currFailedEvaluators = Interlocked.Increment(ref _currentFailedEvaluators);
            if (currFailedEvaluators > _allowedFailedEvaluators)
            {
                Exceptions.Throw(new MaximumNumberOfEvaluatorFailuresExceededException(_allowedFailedEvaluators),
                    Logger);
            }

            _serviceAndContextConfigurationProvider.RecordEvaluatorFailureById(value.Id);
            RemovedFailedContext(value);

            bool isMaster = _serviceAndContextConfigurationProvider.IsMasterEvaluatorId(value.Id);

            // If failed evaluator is master then ask for master 
            // evaluator else ask for mapper evaluator
            if (!isMaster)
            {
                Logger.Log(Level.Info, string.Format("Requesting a replacement map Evaluator for {0}", value.Id));
                RequestMapEvaluators(1);
            }
            else
            {
                Logger.Log(Level.Info, string.Format("Requesting a replacement master Evaluator for {0}", value.Id));
                RequestUpdateEvaluator();
            }
        }

        /// <summary>
        /// This is to remove failed context from _activeContexts
        /// More details will be implemented when working on REEF-1251
        /// </summary>
        /// <param name="value"></param>
        private void RemovedFailedContext(IFailedEvaluator value)
        {
            //// The lock might be move to IFailedEvaluator handler when working on REEF-1251
            lock (_lock)
            {
                if (value.FailedContexts != null && value.FailedContexts.Count > 0)
                {
                    if (value.FailedContexts.Count == 1)
                    {
                        var failedContextId = value.FailedContexts[0].Id;
                        if (!_activeContexts.ContainsKey(failedContextId))
                        {
                            var msg = string.Format(CultureInfo.InvariantCulture,
                                "The active context [{0}] attached in IFailedEvaluator [{1}] is not in the Active Contexts collection.",
                                failedContextId,
                                value.Id);
                            Exceptions.Throw(new SystemException(msg), Logger);
                        }
                        else
                        {
                            _activeContexts.Remove(failedContextId);
                        }
                    }
                    else
                    {
                        var msg = string.Format(CultureInfo.InvariantCulture,
                            "There are [{0}] contexts attached in the failed evaluator. Expected number is 1.",
                            value.FailedContexts.Count);
                        Exceptions.Throw(new IMRUSystemException(msg), Logger);
                    }
                }
            }
        }

        /// <summary>
        /// Specifies what to do if Failed Context is received.
        /// An exception is thrown if tasks are not completed.
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(IFailedContext value)
        {
            if (AreIMRUTasksCompleted())
            {
                Logger.Log(Level.Info,
                    string.Format("Context with Id: {0} failed but IMRU task is completed. So ignoring.", value.Id));
                return;
            }
            Exceptions.Throw(new Exception(string.Format("Data Loading Context with Id: {0} failed", value.Id)), Logger);
        }

        /// <summary>
        /// Specifies what to do if a task fails.
        /// We throw the exception and fail IMRU unless IMRU job is already done.
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(IFailedTask value)
        {
            if (AreIMRUTasksCompleted())
            {
                Logger.Log(Level.Info,
                    string.Format("Task with Id: {0} failed but IMRU task is completed. So ignoring.", value.Id));
                return;
            }
            Exceptions.Throw(new Exception(string.Format("Task with Id: {0} failed", value.Id)), Logger);
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        private bool AreIMRUTasksCompleted()
        {
            return _completedTasks.Count >= _totalMappers + 1;
        }

        private string GetTaskIdByEvaluatorId(string evaluatorId)
        {
            return string.Format("{0}-{1}-Version0",
                IMRUConstants.MapTaskPrefix,
                _serviceAndContextConfigurationProvider.GetPartitionIdByEvaluatorId(evaluatorId));
        }

        /// <summary>
        /// Shuts down evaluators once all completed task messages are received
        /// </summary>
        private void ShutDownAllEvaluators()
        {
            foreach (var task in _completedTasks)
            {
                Logger.Log(Level.Info, string.Format("Disposing task: {0}", task.Id));
                task.ActiveContext.Dispose();
            }
        }

        /// <summary>
        /// Generates map task configuration given the active context. 
        /// Merge configurations of all the inputs to the MapTaskHost.
        /// </summary>
        /// <param name="activeContext">Active context to which task needs to be submitted</param>
        /// <param name="taskId">Task Id</param>
        /// <returns>Map task configuration</returns>
        private IConfiguration GetMapTaskConfiguration(IActiveContext activeContext, string taskId)
        {
            IConfiguration mapSpecificConfig;

            if (!_perMapperConfiguration.TryPop(out mapSpecificConfig))
            {
                Exceptions.Throw(
                    new IMRUSystemException(string.Format("No per map configuration exist for the active context {0}",
                        activeContext.Id)),
                    Logger);
            }

            return TangFactory.GetTang()
                .NewConfigurationBuilder(TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<MapTaskHost<TMapInput, TMapOutput>>.Class)
                    .Build(),
                    _configurationManager.MapFunctionConfiguration,
                    mapSpecificConfig,
                    GetGroupCommConfiguration())
                .BindNamedParameter<InvokeGC, bool>(GenericType<InvokeGC>.Class, _invokeGC.ToString())
                .Build();
        }

        /// <summary>
        /// Generates the update task configuration.
        /// Merge configurations of all the inputs to the UpdateTaskHost.
        /// </summary>
        /// <returns>Update task configuration</returns>
        private IConfiguration GetUpdateTaskConfiguration()
        {
            var partialTaskConf =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier,
                            IMRUConstants.UpdateTaskName)
                        .Set(TaskConfiguration.Task,
                            GenericType<UpdateTaskHost<TMapInput, TMapOutput, TResult>>.Class)
                        .Build(),
                        _configurationManager.UpdateFunctionConfiguration,
                        _configurationManager.ResultHandlerConfiguration,
                        GetGroupCommConfiguration())
                    .BindNamedParameter<InvokeGC, bool>(GenericType<InvokeGC>.Class, _invokeGC.ToString())
                    .Build();

            // This piece of code basically checks if user has given any implementation 
            // of IIMRUResultHandler. If not then bind it to default implementation which 
            // does nothing. For interfaces with generic type we cannot assign default 
            // implementation.
            try
            {
                TangFactory.GetTang()
                    .NewInjector(partialTaskConf)
                    .GetInstance<IIMRUResultHandler<TResult>>();
            }
            catch (InjectionException)
            {
                partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(partialTaskConf)
                    .BindImplementation(GenericType<IIMRUResultHandler<TResult>>.Class,
                        GenericType<DefaultResultHandler<TResult>>.Class)
                    .Build();
                Logger.Log(Level.Info,
                    "User has not given any way to handle IMRU result, defaulting to ignoring it");
            }
            return partialTaskConf;
        }

        /// <summary>
        /// Generate the group communicaiton configuration to be added 
        /// to the tasks
        /// </summary>
        /// <returns>The group communication configuration</returns>
        private IConfiguration GetGroupCommConfiguration()
        {
            var codecConfig =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(
                        StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Conf.Set(
                            StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Codec,
                            GenericType<MapInputWithControlMessageCodec<TMapInput>>.Class).Build(),
                        StreamingCodecConfigurationMinusMessage<TMapOutput>.Conf.Build(),
                        _configurationManager.UpdateFunctionCodecsConfiguration)
                    .Build();

            return Configurations.Merge(_groupCommDriver.GetServiceConfiguration(), codecConfig);
        }

        /// <summary>
        /// Adds broadcast and reduce operators to the default communication group
        /// </summary>
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
                _groupCommDriver.NewCommunicationGroup(IMRUConstants.CommunicationGroupName, _totalMappers + 1)
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

        /// <summary>
        /// Construct the stack of map configuraion which 
        /// is specific to each mapper. If user does not 
        /// specify any then its empty configuration
        /// </summary>
        /// <param name="totalMappers">Total mappers</param>
        /// <returns>Stack of configuration</returns>
        private ConcurrentStack<IConfiguration> ConstructPerMapperConfigStack(int totalMappers)
        {
            var perMapperConfiguration = new ConcurrentStack<IConfiguration>();
            for (int i = 0; i < totalMappers; i++)
            {
                var emptyConfig = TangFactory.GetTang().NewConfigurationBuilder().Build();
                IConfiguration config = _perMapperConfigs.Aggregate(emptyConfig,
                    (current, configGenerator) =>
                        Configurations.Merge(current, configGenerator.GetMapperConfiguration(i, totalMappers)));
                perMapperConfiguration.Push(config);
            }
            return perMapperConfiguration;
        }

        /// <summary>
        /// Request map evaluators from resource manager
        /// </summary>
        /// <param name="numEvaluators">Number of evaluators to request</param>
        private void RequestMapEvaluators(int numEvaluators)
        {
            _evaluatorRequestor.Submit(
                _evaluatorRequestor.NewBuilder()
                    .SetMegabytes(_memoryPerMapper)
                    .SetNumber(numEvaluators)
                    .SetCores(_coresPerMapper)
                    .Build());
        }

        /// <summary>
        /// Request update/master evaluator from resource manager
        /// </summary>
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
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
    /// <typeparam name="TDataHandler">Data Handler type in IInputPartition</typeparam>
    internal sealed class IMRUDriver<TMapInput, TMapOutput, TResult, TDataHandler> : IObserver<IDriverStarted>,
        IObserver<IAllocatedEvaluator>,
        IObserver<IActiveContext>,
        IObserver<ICompletedTask>,
        IObserver<IFailedEvaluator>,
        IObserver<IFailedContext>,
        IObserver<IFailedTask>,
        IObserver<IRunningTask>
    {
        private static readonly Logger Logger =
            Logger.GetLogger(typeof(IMRUDriver<TMapInput, TMapOutput, TResult, TDataHandler>));

        private readonly ConfigurationManager _configurationManager;
        private readonly int _totalMappers;
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private ICommunicationGroupDriver _commGroup;
        private readonly IGroupCommDriver _groupCommDriver;
        private readonly TaskStarter _groupCommTaskStarter;
        private readonly ConcurrentStack<IConfiguration> _perMapperConfiguration;
        private readonly int _coresPerMapper;
        private readonly int _coresForUpdateTask;
        private readonly int _memoryPerMapper;
        private readonly int _memoryForUpdateTask;
        private readonly ISet<IPerMapperConfigGenerator> _perMapperConfigs;
        private readonly IList<ICompletedTask> _completedTasks;
        private readonly int _allowedFailedEvaluators;
        private int _currentFailedEvaluators = 0;
        private readonly bool _invokeGC;
        private bool _imruDone = false;
        private int _taskReady = 0;
        private readonly object _lock = new object();

        private readonly ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TDataHandler>
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
            _completedTasks = new List<ICompletedTask>();

            _allowedFailedEvaluators = (int)(failedEvaluatorsFraction * dataSet.Count);
            _invokeGC = invokeGC;

            AddGroupCommunicationOperators();
            _groupCommTaskStarter = new TaskStarter(_groupCommDriver, _totalMappers + 1);
            _perMapperConfiguration = ConstructPerMapperConfigStack(_totalMappers);
            _serviceAndContextConfigurationProvider =
                new ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TDataHandler>(dataSet);

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
                _serviceAndContextConfigurationProvider.GetNextContextConfiguration(allocatedEvaluator.Id);
            allocatedEvaluator.SubmitContextAndService(configs.Context, configs.Service);
        }

        /// <summary>
        /// Specifies the Map or Update task to run on the active context
        /// </summary>
        /// <param name="activeContext"></param>
        public void OnNext(IActiveContext activeContext)
        {
            Logger.Log(Level.Verbose, string.Format("Received Active Context {0}", activeContext.Id));

            if (_serviceAndContextConfigurationProvider.IsMasterContext(activeContext.Id))
            {
                Logger.Log(Level.Info, "Submitting master task");
                var groupCommConfig = GetGroupCommConfiguration();
                _commGroup.AddTask(IMRUConstants.UpdateTaskName);
                var clientTaskConfig = Configurations.Merge(groupCommConfig, GetUpdateTaskConfiguration());
                _groupCommTaskStarter.QueueTask(clientTaskConfig, activeContext);
                RequestMapEvaluators(_totalMappers);
            }
            else
            {
                Logger.Log(Level.Info, "Submitting map task");
                _serviceAndContextConfigurationProvider.ReachedActiveContext(activeContext.EvaluatorId);
                var groupCommConfig = GetGroupCommConfiguration();
                string taskId = string.Format("{0}-{1}-Version0",
                    IMRUConstants.MapTaskPrefix,
                    _serviceAndContextConfigurationProvider.GetPartitionId(activeContext.EvaluatorId));
                _commGroup.AddTask(taskId);
                var clientTaskConfig = Configurations.Merge(groupCommConfig,
                    GetMapTaskConfiguration(activeContext, taskId));
                _groupCommTaskStarter.QueueTask(clientTaskConfig, activeContext);
                Interlocked.Increment(ref _taskReady);
                Logger.Log(Level.Info, string.Format("{0} Tasks are ready for submission", _taskReady));
            }
        }

        /// <summary>
        /// Specfies what to do when the task is completed
        /// In this case just disposes off the task
        /// </summary>
        /// <param name="completedTask">The link to the completed task</param>
        public void OnNext(ICompletedTask completedTask)
        {
            lock (_lock)
            {
                Logger.Log(Level.Info,
                    string.Format("Received completed task message from task Id: {0}", completedTask.Id));
                _completedTasks.Add(completedTask);

                if (_completedTasks.Count != _totalMappers + 1)
                {
                    return;
                }

                _imruDone = true;
                foreach (var task in _completedTasks)
                {
                    Logger.Log(Level.Info, string.Format("Disposing task: {0}", task.Id));
                    task.ActiveContext.Dispose();
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
            if (_imruDone)
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
                Exceptions.Throw(new Exception("Cannot continue IMRU job, Failed evaluators reach maximum limit"),
                    Logger);
            }

            bool isMaster = _serviceAndContextConfigurationProvider.EvaluatorFailed(value.Id);

            // if active context stage is reached for Update Task then assume that failed
            // evaluator belongs to mapper
            if (isMaster)
            {
                Logger.Log(Level.Info, "Requesting for the failed map evaluator again");
                RequestMapEvaluators(1);
            }
            else
            {
                Logger.Log(Level.Info, "Requesting for the failed master evaluator again");
                RequestUpdateEvaluator();
            }
        }

        /// <summary>
        /// Specified what to do if Failed Context is received.
        /// An exception is thrown if tasks are not completed.
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(IFailedContext value)
        {
            if (_imruDone)
            {
                Logger.Log(Level.Info,
                    string.Format("Context with Id: {0} failed but IMRU task is completed. So ignoring.", value.Id));
                return;
            }
            Exceptions.Throw(new Exception(string.Format("Data Loading Context with Id: {0} failed", value.Id)), Logger);
        }

        /// <summary>
        /// Specifies what to do if a task fails.
        /// An exception is thrown if IMRU job is already done.
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(IFailedTask value)
        {
            if (_imruDone)
            {
                Logger.Log(Level.Info,
                    string.Format("Task with Id: {0} failed but IMRU task is completed. So ignoring.", value.Id));
                return;
            }
            Exceptions.Throw(new Exception(string.Format("Task with Id: {0} failed", value.Id)), Logger);
        }

        /// <summary>
        /// Specified what to do once we receive a running task
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(IRunningTask value)
        {
            Logger.Log(Level.Info, string.Format("Received Running Task message from task with Id: {0}", value.Id));
        }

        /// <summary>
        /// Specfies how to handle exception or error
        /// </summary>
        /// <param name="error">Kind of exception</param>
        public void OnError(Exception error)
        {
            Logger.Log(Level.Error, "Cannot currently handle the Exception in OnError function");
            throw new NotImplementedException("Cannot currently handle exception in OneError", error);
        }

        /// <summary>
        /// Specfies what to do when driver is done
        /// In this case do nothing
        /// </summary>
        public void OnCompleted()
        {
        }

        private IConfiguration GetMapTaskConfiguration(IActiveContext activeContext, string taskId)
        {
            IConfiguration mapSpecificConfig;

            if (!_perMapperConfiguration.TryPop(out mapSpecificConfig))
            {
                Exceptions.Throw(
                    new Exception(string.Format("No per map configuration exist for the active context {0}",
                        activeContext.Id)),
                    Logger);
            }

            return TangFactory.GetTang()
                .NewConfigurationBuilder(TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<MapTaskHost<TMapInput, TMapOutput>>.Class)
                    .Build(),
                    _configurationManager.MapFunctionConfiguration,
                    mapSpecificConfig)
                .BindNamedParameter(typeof(InvokeGC), _invokeGC.ToString())
                .Build();
        }

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
                        _configurationManager.ResultHandlerConfiguration)
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
            return partialTaskConf;
        }

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

            var serviceConf = Configurations.Merge(_groupCommDriver.GetServiceConfiguration(), codecConfig);
            return serviceConf;
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

        private ConcurrentStack<IConfiguration> ConstructPerMapperConfigStack(int totalMappers)
        {
            int counter = 0;
            var perMapperConfiguration = new ConcurrentStack<IConfiguration>();
            for (int i = 0; i < totalMappers; i++)
            {
                var emptyConfig = TangFactory.GetTang().NewConfigurationBuilder().Build();
                IConfiguration config = _perMapperConfigs.Aggregate(emptyConfig,
                    (current, configGenerator) =>
                        Configurations.Merge(current, configGenerator.GetMapperConfiguration(counter, totalMappers)));
                perMapperConfiguration.Push(config);
                counter++;
            }
            return perMapperConfiguration;
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
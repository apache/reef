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
using System.IO;
using System.Linq;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Examples.MachineLearning.KMeans.codecs;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Network.NetworkService.Codec;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Group.Topology;

namespace Org.Apache.REEF.Examples.MachineLearning.KMeans
{
    public class KMeansDriverHandlers : 
        IObserver<IAllocatedEvaluator>,
        IObserver<IActiveContext>, 
        IObserver<IDriverStarted>
    {
        private static readonly Logger _Logger = Logger.GetLogger(typeof(KMeansDriverHandlers));
        private readonly object _lockObj = new object();
        private readonly string _executionDirectory;

        // TODO: we may want to make this injectable
        private readonly int _clustersNumber = 3;
        private readonly int _totalEvaluators;
        private int _partitionInex = 0;
        private readonly IGroupCommDriver _groupCommDriver;
        private readonly ICommunicationGroupDriver _commGroup;
        private readonly TaskStarter _groupCommTaskStarter;
        private readonly IConfiguration _centroidCodecConf;
        private readonly IConfiguration _controlMessageCodecConf;
        private readonly IConfiguration _processedResultsCodecConf;
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        [Inject]
        public KMeansDriverHandlers(
            [Parameter(typeof(NumPartitions))] int numPartitions, 
            GroupCommDriver groupCommDriver,
            IEvaluatorRequestor evaluatorRequestor)
        {
            _executionDirectory = Path.Combine(Directory.GetCurrentDirectory(), Constants.KMeansExecutionBaseDirectory, Guid.NewGuid().ToString("N").Substring(0, 4));
            ISet<string> arguments = ClrHandlerHelper.GetCommandLineArguments();
            string dataFile = arguments.Single(a => a.StartsWith("DataFile", StringComparison.Ordinal)).Split(':')[1];
            DataVector.ShuffleDataAndGetInitialCentriods(
                Path.Combine(Directory.GetCurrentDirectory(), "reef", "global", dataFile),
                numPartitions,
                _clustersNumber,
                _executionDirectory);

            _totalEvaluators = numPartitions + 1;

            _groupCommDriver = groupCommDriver;
            _evaluatorRequestor = evaluatorRequestor;

            _centroidCodecConf = CodecToStreamingCodecConfiguration<Centroids>.Conf
                .Set(CodecConfiguration<Centroids>.Codec, GenericType<CentroidsCodec>.Class)
                .Build();

            IConfiguration dataConverterConfig1 = PipelineDataConverterConfiguration<Centroids>.Conf
                .Set(PipelineDataConverterConfiguration<Centroids>.DataConverter, GenericType<DefaultPipelineDataConverter<Centroids>>.Class)
                .Build();

            _controlMessageCodecConf = CodecToStreamingCodecConfiguration<ControlMessage>.Conf
                .Set(CodecConfiguration<ControlMessage>.Codec, GenericType<ControlMessageCodec>.Class)
                .Build();

            IConfiguration dataConverterConfig2 = PipelineDataConverterConfiguration<ControlMessage>.Conf
                .Set(PipelineDataConverterConfiguration<ControlMessage>.DataConverter, GenericType<DefaultPipelineDataConverter<ControlMessage>>.Class)
                .Build();

            _processedResultsCodecConf = CodecToStreamingCodecConfiguration<ProcessedResults>.Conf
                .Set(CodecConfiguration<ProcessedResults>.Codec, GenericType<ProcessedResultsCodec>.Class)
                .Build();

            IConfiguration reduceFunctionConfig = ReduceFunctionConfiguration<ProcessedResults>.Conf
                .Set(ReduceFunctionConfiguration<ProcessedResults>.ReduceFunction, GenericType<KMeansMasterTask.AggregateMeans>.Class)
                .Build();

            IConfiguration dataConverterConfig3 = PipelineDataConverterConfiguration<ProcessedResults>.Conf
                .Set(PipelineDataConverterConfiguration<ProcessedResults>.DataConverter, GenericType<DefaultPipelineDataConverter<ProcessedResults>>.Class)
                .Build();

            _commGroup = _groupCommDriver.DefaultGroup
                   .AddBroadcast<Centroids>(Constants.CentroidsBroadcastOperatorName, Constants.MasterTaskId, TopologyTypes.Flat, dataConverterConfig1)
                   .AddBroadcast<ControlMessage>(Constants.ControlMessageBroadcastOperatorName, Constants.MasterTaskId, TopologyTypes.Flat, dataConverterConfig2)
                   .AddReduce<ProcessedResults>(Constants.MeansReduceOperatorName, Constants.MasterTaskId, TopologyTypes.Flat, reduceFunctionConfig, dataConverterConfig3)
                   .Build();

            _groupCommTaskStarter = new TaskStarter(_groupCommDriver, _totalEvaluators);
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            IConfiguration contextConfiguration = _groupCommDriver.GetContextConfiguration();

            int partitionNum;
            if (_groupCommDriver.IsMasterContextConfiguration(contextConfiguration))
            {
                partitionNum = -1;
            }
            else
            {
                lock (_lockObj)
                {
                    partitionNum = _partitionInex;
                    _partitionInex++;
                }
            }

            IConfiguration gcServiceConfiguration = _groupCommDriver.GetServiceConfiguration();
            gcServiceConfiguration = Configurations.Merge(gcServiceConfiguration, _centroidCodecConf, _controlMessageCodecConf, _processedResultsCodecConf);
            IConfiguration commonServiceConfiguration = TangFactory.GetTang().NewConfigurationBuilder(gcServiceConfiguration)
                .BindNamedParameter<DataPartitionCache.PartitionIndex, int>(GenericType<DataPartitionCache.PartitionIndex>.Class, partitionNum.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<KMeansConfiguratioinOptions.ExecutionDirectory, string>(GenericType<KMeansConfiguratioinOptions.ExecutionDirectory>.Class, _executionDirectory)
                .BindNamedParameter<KMeansConfiguratioinOptions.TotalNumEvaluators, int>(GenericType<KMeansConfiguratioinOptions.TotalNumEvaluators>.Class, _totalEvaluators.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<KMeansConfiguratioinOptions.K, int>(GenericType<KMeansConfiguratioinOptions.K>.Class, _clustersNumber.ToString(CultureInfo.InvariantCulture))
                .Build();

            IConfiguration dataCacheServiceConfiguration = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services, GenericType<DataPartitionCache>.Class)
                .Build();
            
            allocatedEvaluator.SubmitContextAndService(contextConfiguration, Configurations.Merge(commonServiceConfiguration, dataCacheServiceConfiguration));
        }

        public void OnNext(IActiveContext activeContext)
        {
            IConfiguration taskConfiguration;

            if (_groupCommDriver.IsMasterTaskContext(activeContext))
            {
                // Configure Master Task
                taskConfiguration = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, Constants.MasterTaskId)
                    .Set(TaskConfiguration.Task, GenericType<KMeansMasterTask>.Class)
                    .Build();

                _commGroup.AddTask(Constants.MasterTaskId);
            }
            else
            {
                string slaveTaskId = Constants.SlaveTaskIdPrefix + activeContext.Id;

                // Configure Slave Task
                taskConfiguration = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, Constants.SlaveTaskIdPrefix + activeContext.Id)
                    .Set(TaskConfiguration.Task, GenericType<KMeansSlaveTask>.Class)
                    .Build();

                _commGroup.AddTask(slaveTaskId);
            }
            _groupCommTaskStarter.QueueTask(taskConfiguration, activeContext);
        }

        public void OnNext(IDriverStarted value)
        {
            var request = _evaluatorRequestor.NewBuilder().SetCores(1).SetMegabytes(2048).Build();

            _evaluatorRequestor.Submit(request);
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }

    [NamedParameter("Number of partitions")]
    public class NumPartitions : Name<int>
    {
    }
}

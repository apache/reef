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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Demo.Examples;
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Demo.Driver
{
    /// <summary>
    /// Rough implementation of IDataSetMaster that supports loading new datasets, one at a time.
    /// Allocates one partition per one evaluator.
    /// </summary>
   [DriverSide]
    public sealed class DataSetMaster : IDataSetMaster, IObserver<IAllocatedEvaluator>, IObserver<IActiveContext>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DataSetMaster));

        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private readonly IPartitionDescriptorFetcher _partitionDescriptorFetcher;
        private readonly AvroConfigurationSerializer _avroConfigurationSerializer;
        private readonly ConcurrentDictionary<string, string> _contextIdToPartitionId;
        private readonly PartitionCollector _partitionCollector;
        private readonly ConcurrentQueue<IConfiguration> _partitionConfigurations;
        private readonly SynchronizedCollection<PartitionInfo> _partitionInfos;
        private readonly CountdownEvent _countdownEvent;

        private string _dataSetId;

        [Inject]
        private DataSetMaster(IEvaluatorRequestor evaluatorRequestor,
                              IPartitionDescriptorFetcher partitionDescriptorFetcher,
                              AvroConfigurationSerializer avroConfigurationSerializer,
                              PartitionCollector partitionCollector)
        {
            _evaluatorRequestor = evaluatorRequestor;
            _partitionDescriptorFetcher = partitionDescriptorFetcher;
            _avroConfigurationSerializer = avroConfigurationSerializer;
            _contextIdToPartitionId = new ConcurrentDictionary<string, string>();
            _partitionCollector = partitionCollector;
            _partitionConfigurations = new ConcurrentQueue<IConfiguration>();
            _partitionInfos = new SynchronizedCollection<PartitionInfo>();
            _countdownEvent = new CountdownEvent(0);
        }

        public IDataSet<T> Load<T>(Uri uri)
        {
            _dataSetId = string.Format("DataSet-{0}", Guid.NewGuid().ToString("N").Substring(0, 8));
            Logger.Log(Level.Info, "Load dataset {0} from uri {1}", _dataSetId, uri);

            foreach (var partitionDescriptor in _partitionDescriptorFetcher.GetPartitionDescriptors(uri))
            {
                var incompletePartitionConf = partitionDescriptor.GetPartitionConfiguration();
                var partitionConf = TangFactory.GetTang().NewConfigurationBuilder(incompletePartitionConf)
                    .BindNamedParameter<InputPartitionId, string>(GenericType<InputPartitionId>.Class,
                        partitionDescriptor.Id)
                    .Build();
                _partitionConfigurations.Enqueue(partitionConf);
            }

            Logger.Log(Level.Info, "Submit evaluators for {0}", _dataSetId);
            _countdownEvent.Reset(_partitionConfigurations.Count);

            _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder()
                .SetNumber(_partitionConfigurations.Count)
                .SetMegabytes(1024) // TODO: replace with some named parameter
                .Build());

            Logger.Log(Level.Info, "Waiting evaluators for {0}", _dataSetId);
            _countdownEvent.Wait();
            Logger.Log(Level.Info, "Done waiting evaluators for {0}", _dataSetId);

            return new DataSet<T>(_dataSetId, new DataSetInfo(_dataSetId, _partitionInfos.ToArray()), _partitionCollector);
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            Logger.Log(Level.Info, "Got new evaluator {0}", allocatedEvaluator.Id);

            if (_dataSetId == null)
            {
                Logger.Log(Level.Warning, "Dataset id should be known by this point, but wasn't found. Will release evaluator {0} immediately.", allocatedEvaluator.Id);
                allocatedEvaluator.Dispose();
                return;
            }

            IConfiguration partitionConf;
            if (!_partitionConfigurations.TryDequeue(out partitionConf))
            {
                Logger.Log(Level.Warning, "Found no dataset partition descriptors for {0}. Will release evaluator immediately.", allocatedEvaluator.Id);
                allocatedEvaluator.Dispose();
                return;
            }

            IInjector injector = TangFactory.GetTang().NewInjector(partitionConf);

            string partitionId = injector.GetNamedInstance<InputPartitionId, string>();
            IConfiguration serviceConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<SerializedInitialDataLoadPartitions, string>(
                    GenericType<SerializedInitialDataLoadPartitions>.Class,
                    _avroConfigurationSerializer.ToString(partitionConf))
                .BindNamedParameter(typeof(LoadedDataSetIdNamedParameter), _dataSetId)
                .Build();

            string contextId = string.Format("DataSetLoadContext-{0}", partitionId);
            IConfiguration contextConf = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, contextId)
                .Set(ContextConfiguration.OnContextStart, GenericType<DataSetLoadContext>.Class)
                .Set(ContextConfiguration.OnSendMessage, GenericType<PartitionReporter>.Class)
                .Build();

            _contextIdToPartitionId[contextId] = partitionId;

            allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
        }

        public void OnNext(IActiveContext activeContext)
        {
            Logger.Log(Level.Info, "Got {0}", activeContext);

            if (_dataSetId == null)
            {
                Logger.Log(Level.Warning, "Dataset id should be known by this point, but wasn't found. Will release context {0} immediately.", activeContext.Id);
                activeContext.Dispose();
                return;
            }

            string partitionId = _contextIdToPartitionId[activeContext.Id];
            _partitionInfos.Add(new PartitionInfo(partitionId, activeContext));

            Logger.Log(Level.Info, "Signal CountDownLatch for dataset {0} from partition {1}", _dataSetId, partitionId);
            _countdownEvent.Signal();

        }

        public void Store<T>(IDataSet<T> dataSet)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception e)
        {
            throw e;
        }
    }
}

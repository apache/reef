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
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Demo.Driver
{
    /// <summary>
    /// Rough implementation of IDataSetMaster with very inefficient synchronization support for loading multiple datasets concurrently.
    /// Allocates one partition per one evaluator.
    /// </summary>
    public sealed class DataSetMaster : IDataSetMaster, IObserver<IAllocatedEvaluator>, IObserver<IActiveContext>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DataSetMaster));

        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private readonly IPartitionDescriptorFetcher _partitionDescriptorFetcher;
        private readonly AvroConfigurationSerializer _avroConfigurationSerializer;
        private readonly IDictionary<string, Queue<IConfiguration>> _partitionConfigurationsForDatasets;

        [Inject]
        private DataSetMaster(IEvaluatorRequestor evaluatorRequestor,
                              IPartitionDescriptorFetcher partitionDescriptorFetcher,
                              AvroConfigurationSerializer avroConfigurationSerializer)
        {
            _evaluatorRequestor = evaluatorRequestor;
            _partitionDescriptorFetcher = partitionDescriptorFetcher;
            _avroConfigurationSerializer = avroConfigurationSerializer;
            _partitionConfigurationsForDatasets = new Dictionary<string, Queue<IConfiguration>>();
        }

        public IDataSet<T> Load<T>(Uri uri)
        {
            string dataSetId = uri.ToString();
            Logger.Log(Level.Info, "Load data from {0}", dataSetId);

            Queue<IConfiguration> partitionConfigurations = new Queue<IConfiguration>();
            _partitionConfigurationsForDatasets[dataSetId] = partitionConfigurations;
            foreach (var partitionDescriptor in _partitionDescriptorFetcher.GetPartitionDescriptors(uri))
            {
                var partitionConf = partitionDescriptor.GetPartitionConfiguration();
                var finalPartitionConf = TangFactory.GetTang().NewConfigurationBuilder(partitionConf)
                    .BindNamedParameter<DataPartitionId, string>(GenericType<DataPartitionId>.Class,
                        partitionDescriptor.Id)
                    .Build();
                partitionConfigurations.Enqueue(finalPartitionConf);
            }

            lock (partitionConfigurations)
            {
                _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder()
                    .SetNumber(partitionConfigurations.Count)
                    .SetMegabytes(1024)
                    .Build());

                // The idea is to wait until all evaluators have been loaded with partitions,
                // then return with the appropriate IDataSet object.
                // However, the line below blocks OnNext(IAllocatedEvaluator allocatedEvaluator) from firing, for some reason.

                // Monitor.Wait(partitionConfigurations);
            }

            return null; // must return IDataSet
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            Logger.Log(Level.Info, "Got {0}", allocatedEvaluator);

            IConfiguration partitionConf = null;
            foreach (KeyValuePair<string, Queue<IConfiguration>> pair in _partitionConfigurationsForDatasets)
            {
                string dataSetId = pair.Key;
                Queue<IConfiguration> partitionConfigurations = pair.Value;
                lock (partitionConfigurations)
                {
                    if (partitionConfigurations.Count == 0)
                    {
                        //// this dataset has already been given enough evaluators
                        continue;
                    }

                    partitionConf = partitionConfigurations.Dequeue();
                    if (partitionConfigurations.Count == 0)
                    {
                        Logger.Log(Level.Verbose,
                            "Dataset {0} has been given enough evaluators. Will remove from dictionary.",
                            dataSetId);
                        _partitionConfigurationsForDatasets.Remove(dataSetId);

                        Monitor.Pulse(partitionConfigurations);
                    }
                    break;
                }
            }

            if (partitionConf == null)
            {
                Logger.Log(Level.Warning, "Found no dataset partition descriptors for {0}. Will release evaluator immediately.", allocatedEvaluator.Id);
                allocatedEvaluator.Dispose();
                return;
            }

            IConfiguration serviceConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<SerializedInitialDataLoadPartitions, string>(
                    GenericType<SerializedInitialDataLoadPartitions>.Class,
                    _avroConfigurationSerializer.ToString(partitionConf))
                .Build();

            IConfiguration contextConf = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, "Context-" + allocatedEvaluator.Id)
                .Set(ContextConfiguration.OnContextStart, GenericType<DataLoadContext>.Class)
                .Build();

            allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
        }

        public void OnNext(IActiveContext activeContext)
        {
            Logger.Log(Level.Info, "Got {0}", activeContext);

            // must pass this activeContext to the user
            // For now, just dispose contexts right away for demonstration
            activeContext.Dispose();
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

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
using System.Globalization;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Class that provides Service and Context configuration
    /// </summary>
    /// <typeparam name="TMapInput"></typeparam>
    /// <typeparam name="TMapOutput"></typeparam>
    /// <typeparam name="TPartitionType"></typeparam>
    [NotThreadSafe]
    internal sealed class ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TPartitionType>
    {
        private static readonly Logger Logger 
            = Logger.GetLogger(typeof(ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TPartitionType>));

        /// <summary>
        /// Mapping between Evaluator id and assigned partition descriptor/context ids
        /// </summary>
        private readonly Dictionary<string, PartitionDescriptorContextIdBundle> _partitionContextIdProvider 
            = new Dictionary<string, PartitionDescriptorContextIdBundle>();

        /// <summary>
        /// Available partition descriptor and context ids stack
        /// </summary>
        private readonly Stack<PartitionDescriptorContextIdBundle> _availablePartitionDescriptorContextIds
            = new Stack<PartitionDescriptorContextIdBundle>();

        /// <summary>
        /// Input partition data set
        /// </summary>
        private readonly IPartitionedInputDataSet _dataset;

        /// <summary>_configurationManager
        /// Configuration manager that provides configurations
        /// </summary>
        private readonly ConfigurationManager _configurationManager;

        /// <summary>
        /// Constructs the object which maintains partitionDescriptor Ids so that to provide proper data load configuration
        /// It also maintains the partitionDescriptor id and context id mapping to ensure same context id alway assign the same data partition
        /// This is to ensure if the tasks are added to the typology based on the sequence of context id, the result is deterministic. 
        /// </summary>
        /// <param name="dataset">partition input dataset</param>
        /// <param name="configurationManager">Configuration manager that holds configurations for context and tasks</param>
        internal ServiceAndContextConfigurationProvider(IPartitionedInputDataSet dataset, ConfigurationManager configurationManager)
        {
            _dataset = dataset;
            int contextSequenceNumber = 0;
            foreach (var descriptor in _dataset)
            {
                var contextId = string.Format("DataLoadingContext-{0}", contextSequenceNumber++);
                _availablePartitionDescriptorContextIds.Push(new PartitionDescriptorContextIdBundle(descriptor.Id, contextId));
            }
            _configurationManager = configurationManager;
        }

        /// <summary>
        /// Handles failed evaluator. Push the partitionId back to Partition Descriptor Id stack and 
        /// remove evaluatorId from Partition Id Provider collection
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns>Whether failed evaluator is master or not</returns>
        internal void RemoveEvaluatorIdFromPartitionIdProvider(string evaluatorId)
        {
            PartitionDescriptorContextIdBundle partitionDescriptor;
            if (_partitionContextIdProvider.TryGetValue(evaluatorId, out partitionDescriptor))
            {
                _availablePartitionDescriptorContextIds.Push(partitionDescriptor);
                _partitionContextIdProvider.Remove(evaluatorId);
            }
            else
            {
                var msg = string.Format(CultureInfo.InvariantCulture,
                    "Partition descriptor for Failed evaluator:{0} not present",
                    evaluatorId);
                throw new Exception(msg);
            }
        }

        /// <summary>
        /// Gets Context and Service configuration for Master
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        internal ContextAndServiceConfiguration GetContextConfigurationForMasterEvaluatorById(string evaluatorId)
        {
            Logger.Log(Level.Info, "Getting root context and service configuration for master");

            var serviceConf = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services, GenericType<TaskStateService>.Class)
                .Build();

            return new ContextAndServiceConfiguration(
                ContextConfiguration.ConfigurationModule.Set(ContextConfiguration.Identifier,
                    IMRUConstants.MasterContextId).Build(),
                Configurations.Merge(serviceConf, _configurationManager.UpdateTaskStateConfiguration));
        }

        /// <summary>
        /// Gets partition Id for the evaluator
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        internal string GetPartitionIdByEvaluatorId(string evaluatorId)
        {
            PartitionDescriptorContextIdBundle partitionDescriptorContextId;
            _partitionContextIdProvider.TryGetValue(evaluatorId, out partitionDescriptorContextId);

            if (partitionDescriptorContextId == null)
            {
                var msg = string.Format(CultureInfo.InvariantCulture, 
                    "Partition descriptor for evaluator:{0} is not present in the mapping", evaluatorId);
                throw new IMRUSystemException(msg);
            }
            return partitionDescriptorContextId.PartitionDescriptorId;
        }

        /// <summary>
        /// Gives context and service configuration for next evaluator either from failed 
        /// evaluator or new configuration
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns>Configuration for context and service</returns>
        internal ContextAndServiceConfiguration GetDataLoadingConfigurationForEvaluatorById(string evaluatorId)
        {
            try
            {
                Logger.Log(Level.Info, "Getting a new data loading configuration");
                _partitionContextIdProvider.Add(evaluatorId, _availablePartitionDescriptorContextIds.Pop());
            }
            catch (InvalidOperationException e)
            {
                throw new IMRUSystemException("No more data configuration can be provided", e);
            }
            catch (ArgumentException e)
            {
                var msg =
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "Evaluator Id:{0} already present in configuration cache, they have to be unique",
                        evaluatorId);
                throw new IMRUSystemException(msg, e);
            }

            try
            {
                var partitionIdContextId = _partitionContextIdProvider[evaluatorId];
                IPartitionDescriptor partitionDescriptor =
                    _dataset.GetPartitionDescriptorForId(partitionIdContextId.PartitionDescriptorId);
                return GetDataLoadingContextAndServiceConfiguration(partitionDescriptor, partitionIdContextId.ContextId);
            }
            catch (Exception e)
            {
                var msg = string.Format(CultureInfo.InvariantCulture, 
                    "Error while trying to access partition descriptor:{0} from dataset",
                    _partitionContextIdProvider[evaluatorId]);
                Exceptions.Throw(e, msg, Logger);
                return null;
            }
        }

        /// <summary>
        /// Creates service and data loading context configuration for given context id and partition descriptor
        /// </summary>
        /// <param name="partitionDescriptor"></param>
        /// <param name="contextId"></param>
        /// <returns>Configuration for context and service</returns>
        private ContextAndServiceConfiguration GetDataLoadingContextAndServiceConfiguration(
            IPartitionDescriptor partitionDescriptor, string contextId)
        {
            var dataLoadingContextConf =
                TangFactory.GetTang()
                    .NewConfigurationBuilder()
                    .BindSetEntry<ContextConfigurationOptions.StartHandlers, DataLoadingContext<TPartitionType>, IObserver<IContextStart>>(
                            GenericType<ContextConfigurationOptions.StartHandlers>.Class,
                            GenericType<DataLoadingContext<TPartitionType>>.Class)
                    .Build();

            var serviceConf =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(
                        ServiceConfiguration.ConfigurationModule
                        .Set(ServiceConfiguration.Services, GenericType<TaskStateService>.Class)
                        .Build(),
                        dataLoadingContextConf,
                        partitionDescriptor.GetPartitionConfiguration(),
                        _configurationManager.MapTaskStateConfiguration)
                    .Build();

            var contextConf = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, contextId)
                .Build();
            return new ContextAndServiceConfiguration(contextConf, serviceConf);
        }
    }
}
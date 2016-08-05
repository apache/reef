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
// specific language governing permissions and
// under the License.

using System;
using System.Collections.Generic;
using System.Globalization;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.IO.PartitionedData;
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
        private static readonly Logger Logger = Logger.GetLogger(typeof(ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TPartitionType>));

        private readonly Dictionary<string, string> _partitionIdProvider = new Dictionary<string, string>();
        private readonly Stack<string> _partitionDescriptorIds = new Stack<string>();
        private readonly IPartitionedInputDataSet _dataset;

        /// <summary>
        /// Constructs the object witch maintains partitionDescriptor Ids so that to provide proper data load configuration 
        /// </summary>
        /// <param name="dataset"></param>
        internal ServiceAndContextConfigurationProvider(IPartitionedInputDataSet dataset)
        {
            _dataset = dataset;
            foreach (var descriptor in _dataset)
            {
                _partitionDescriptorIds.Push(descriptor.Id);
            }
        }

        /// <summary>
        /// Handles failed evaluator. Push the partitionId back to Partition Descriptor Id stack and 
        /// remove evaluatorId from Partition Id Provider collection
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns>Whether failed evaluator is master or not</returns>
        internal void RemoveEvaluatorIdFromPartitionIdProvider(string evaluatorId)
        {
            if (!_partitionIdProvider.ContainsKey(evaluatorId))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "Partition descriptor for Failed evaluator:{0} not present", evaluatorId);
                Exceptions.Throw(new Exception(msg), Logger);
            }
            _partitionDescriptorIds.Push(_partitionIdProvider[evaluatorId]);
            _partitionIdProvider.Remove(evaluatorId);
        }

        /// <summary>
        /// Gets Context and Service configuration for Master
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        internal ContextAndServiceConfiguration GetContextConfigurationForMasterEvaluatorById(string evaluatorId)
        {
            Logger.Log(Level.Info, "Getting root context and service configuration for master");
            return new ContextAndServiceConfiguration(
                ContextConfiguration.ConfigurationModule.Set(ContextConfiguration.Identifier,
                    IMRUConstants.MasterContextId).Build(),
                TangFactory.GetTang().NewConfigurationBuilder().Build());
        }

        /// <summary>
        /// Gets partition Id for the evaluator
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        internal string GetPartitionIdByEvaluatorId(string evaluatorId)
        {
            if (!_partitionIdProvider.ContainsKey(evaluatorId))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "Partition descriptor for evaluator:{0} is not present in the mapping", evaluatorId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            return _partitionIdProvider[evaluatorId];
        }

        /// <summary>
        /// Gives context and service configuration for next evaluator either from failed 
        /// evaluator or new configuration
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        internal ContextAndServiceConfiguration GetDataLoadingConfigurationForEvaluatorById(string evaluatorId)
        {
            if (_partitionDescriptorIds.Count == 0)
            {
                Exceptions.Throw(new IMRUSystemException("No more data configuration can be provided"), Logger);
            }

            if (_partitionIdProvider.ContainsKey(evaluatorId))
            {
                var msg =
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "Evaluator Id:{0} already present in configuration cache, they have to be unique",
                        evaluatorId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            Logger.Log(Level.Info, "Getting a new data loading configuration");
            _partitionIdProvider[evaluatorId] = _partitionDescriptorIds.Pop();

            try
            {
                IPartitionDescriptor partitionDescriptor =
                    _dataset.GetPartitionDescriptorForId(_partitionIdProvider[evaluatorId]);
                return GetDataLoadingContextAndServiceConfiguration(partitionDescriptor, evaluatorId);
            }
            catch (Exception e)
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "Error while trying to access partition descriptor:{0} from dataset",
                    _partitionIdProvider[evaluatorId]);
                Exceptions.Throw(e, msg, Logger);
                return null;
            }
        }

        /// <summary>
        /// Creates service and data loading context configuration for given evaluator id
        /// </summary>
        /// <param name="partitionDescriptor"></param>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        private static ContextAndServiceConfiguration GetDataLoadingContextAndServiceConfiguration(
            IPartitionDescriptor partitionDescriptor,
            string evaluatorId)
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
                    .NewConfigurationBuilder(ServiceConfiguration.ConfigurationModule.Build(),
                        dataLoadingContextConf,
                        partitionDescriptor.GetPartitionConfiguration())
                    .Build();

            var contextConf = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, string.Format("DataLoading-{0}", evaluatorId))
                .Build();
            return new ContextAndServiceConfiguration(contextConf, serviceConf);
        }
    }
}
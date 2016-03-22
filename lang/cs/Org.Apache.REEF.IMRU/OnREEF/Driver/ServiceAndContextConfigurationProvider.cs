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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Class that handles failed evaluators and also provides Service 
    /// and Context configuration
    /// </summary>
    /// <typeparam name="TMapInput"></typeparam>
    /// <typeparam name="TMapOutput"></typeparam>
    /// <typeparam name="TDataHandler"></typeparam>
    internal sealed class ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TDataHandler>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TDataHandler>));

        private readonly Dictionary<string, string> _configurationProvider;
        private readonly ISet<string> _submittedEvaluators;
        private readonly ISet<string> _contextLoadedEvaluators; 
        private readonly object _lock;
        private readonly Stack<string> _partitionDescriptorIds;
        private readonly IPartitionedInputDataSet _dataset;
        private string _masterEvaluatorId;

        internal ServiceAndContextConfigurationProvider(IPartitionedInputDataSet dataset)
        {
            _configurationProvider = new Dictionary<string, string>();
            _submittedEvaluators = new HashSet<string>();
            _contextLoadedEvaluators = new HashSet<string>();
            _dataset = dataset;
            _partitionDescriptorIds = new Stack<string>();
            foreach (var descriptor in _dataset)
            {
                _partitionDescriptorIds.Push(descriptor.Id);
            }
            _lock = new object();
        }

        /// <summary>
        /// Handles failed evaluator. Moves the id from 
        /// submitted evaluator to failed evaluator
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns>Whether failed evaluator is master or not</returns>
        internal bool EvaluatorFailed(string evaluatorId)
        {
            lock (_lock)
            {
                string msg;
                bool isMaster = IsMaster(evaluatorId);

                if (_contextLoadedEvaluators.Contains(evaluatorId))
                {
                    msg =
                        string.Format(
                            "Failed evaluator:{0} already had context loaded. Cannot handle failure at this stage",
                            evaluatorId);
                    Exceptions.Throw(new Exception(msg), Logger);
                }

                if (!_submittedEvaluators.Contains(evaluatorId))
                {
                    msg = string.Format("Failed evaluator:{0} was never submitted", evaluatorId);
                    Exceptions.Throw(new Exception(msg), Logger);
                }

                if (!_configurationProvider.ContainsKey(evaluatorId) && !isMaster)
                {
                    msg = string.Format("Partition descriptor for Failed evaluator:{0} not present", evaluatorId);
                    Exceptions.Throw(new Exception(msg), Logger);
                }

                _submittedEvaluators.Remove(evaluatorId);

                if (isMaster)
                {
                    Logger.Log(Level.Info, "Failed Evaluator is Master");
                    _masterEvaluatorId = null;
                    return true;
                }
                
                Logger.Log(Level.Info, "Failed Evaluator is a Mapper");
                _partitionDescriptorIds.Push(_configurationProvider[evaluatorId]);
                _configurationProvider.Remove(evaluatorId);
                return false;
            }
        }

        /// <summary>
        /// Notifies that active context state has been reached
        /// </summary>
        /// <param name="evaluatorId"></param>
        internal void ReachedActiveContext(string evaluatorId)
        {
            lock (_lock)
            {
                if (!_submittedEvaluators.Contains(evaluatorId))
                {
                    var msg = string.Format("Evaluator:{0} never loaded data but still reached active context stage",
                        evaluatorId);
                    Exceptions.Throw(new Exception(msg), Logger);
                }

                if (_contextLoadedEvaluators.Contains(evaluatorId))
                {
                    var msg = string.Format("Evaluator:{0} already reached the active context stage", evaluatorId);
                    Exceptions.Throw(new Exception(msg), Logger);
                }

                _contextLoadedEvaluators.Add(evaluatorId);
                _submittedEvaluators.Remove(evaluatorId);
            }
        }

        /// <summary>
        /// Gets next context configuration. Either master or mapper.
        /// </summary>
        /// <param name="evaluatorId">Evaluator Id</param>
        /// <returns>The context and service configuration</returns>
        internal ContextAndServiceConfiguration GetNextContextConfiguration(string evaluatorId)
        {
            lock (_lock)
            {
                if (_submittedEvaluators.Contains(evaluatorId))
                {
                    string msg = string.Format("The context is already submitted to evaluator:{0}", evaluatorId);
                    Exceptions.Throw(new Exception(msg), Logger);
                }

                if (_masterEvaluatorId == null)
                {
                    Logger.Log(Level.Info, "Submitting root context and service for master");
                    _masterEvaluatorId = evaluatorId;
                    _submittedEvaluators.Add(evaluatorId);
                    return new ContextAndServiceConfiguration(
                        ContextConfiguration.ConfigurationModule.Set(ContextConfiguration.Identifier,
                            IMRUConstants.MasterContextId).Build(),
                        TangFactory.GetTang().NewConfigurationBuilder().Build());
                }

                Logger.Log(Level.Info, "Submitting root context and service for a map task");
                return GetNextDataLoadingConfiguration(evaluatorId);
            }
        }

        /// <summary>
        /// Checks whether the context id belongs to master
        /// </summary>
        /// <param name="activeContextId">context id</param>
        /// <returns>true of it is master context id, false otherwise</returns>
        internal bool IsMasterContext(string activeContextId)
        {
            lock (_lock)
            {
                return activeContextId.Equals(IMRUConstants.MasterContextId);
            }
        }

        /// <summary>
        /// Gets partition Id for the evaluator
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        internal string GetPartitionId(string evaluatorId)
        {
            lock (_lock)
            {
                string msg;
                if (!_submittedEvaluators.Contains(evaluatorId) && !_contextLoadedEvaluators.Contains(evaluatorId))
                {
                    msg = string.Format("Context for Evaluator:{0} has never been submitted", evaluatorId);
                    Exceptions.Throw(new Exception(msg), Logger);
                }

                if (IsMaster(evaluatorId))
                {
                    msg = string.Format("Evaluator:{0} is master and does not get partition", evaluatorId);
                    Exceptions.Throw(new Exception(msg), Logger);
                }

                if (!_configurationProvider.ContainsKey(evaluatorId))
                {
                    msg = string.Format("Partition descriptor for evaluator:{0} is not present in the mapping", evaluatorId);
                    Exceptions.Throw(new Exception(msg), Logger);   
                }

                return _configurationProvider[evaluatorId];
            }
        }

        /// <summary>
        /// Gives context and service configuration for next evaluator either from failed 
        /// evaluator or new configuration
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        private ContextAndServiceConfiguration GetNextDataLoadingConfiguration(string evaluatorId)
        {
            string msg;
           
            if (_contextLoadedEvaluators.Contains(evaluatorId))
            {
                msg = string.Format("Evaluator:{0} already has the data loaded", evaluatorId);
                Exceptions.Throw(new Exception(msg), Logger);
            }

            if (_partitionDescriptorIds.Count == 0)
            {
                Exceptions.Throw(new Exception("No more data configuration can be provided"), Logger);
            }

            if (_configurationProvider.ContainsKey(evaluatorId))
            {
                msg =
                    string.Format(
                        "Evaluator Id:{0} already present in configuration cache, they have to be unique",
                        evaluatorId);
                Exceptions.Throw(new Exception(msg), Logger);
            }

            Logger.Log(Level.Info, "Getting a new data loading configuration");
            _configurationProvider[evaluatorId] = _partitionDescriptorIds.Pop();
            _submittedEvaluators.Add(evaluatorId);

            msg = string.Format(
                "Current status: Submitted Evaluators-{0}, Data Loaded Evaluators-{1}, Unused data partitions-{2}",
                _submittedEvaluators.Count,
                _contextLoadedEvaluators.Count,
                _partitionDescriptorIds.Count);
            Logger.Log(Level.Info, msg);

            try
            {
                IPartitionDescriptor partitionDescriptor =
                    _dataset.GetPartitionDescriptorForId(_configurationProvider[evaluatorId]);

                if (partitionDescriptor == null)
                {
                    msg = string.Format("Partition descriptor with Id:{0} does not exist",
                        _configurationProvider[evaluatorId]);
                    Exception e = new NullReferenceException(msg);
                    Exceptions.Throw(e, Logger);
                }
                return GetDataLoadingContextAndServiceConfiguration(partitionDescriptor, evaluatorId);
            }
            catch (Exception e)
            {
                msg = string.Format("Error while trying to access partition descriptor:{0} from dataset",
                    _configurationProvider[evaluatorId]);
                Exceptions.Throw(e, msg, Logger);
                return null;
            }
        }

        private ContextAndServiceConfiguration GetDataLoadingContextAndServiceConfiguration(
            IPartitionDescriptor partitionDescriptor,
            string evaluatorId)
        {
            var dataLoadingContextConf =
                TangFactory.GetTang()
                    .NewConfigurationBuilder()
                    .BindSetEntry<ContextConfigurationOptions.StartHandlers, DataLoadingContext<TDataHandler>, IObserver<IContextStart>>(
                            GenericType<ContextConfigurationOptions.StartHandlers>.Class,
                            GenericType<DataLoadingContext<TDataHandler>>.Class)
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

        private bool IsMaster(string evaluatorId)
        {
            return _masterEvaluatorId.Equals(evaluatorId);
        }
    }
}

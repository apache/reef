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
using System.Linq;
using Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Class that handles failed evaluators and also provides Service 
    /// and Context configuration
    /// </summary>
    /// <typeparam name="TMapInput"></typeparam>
    /// <typeparam name="TMapOutput"></typeparam>
    internal sealed class ServiceAndContextConfigurationProvider<TMapInput, TMapOutput>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ServiceAndContextConfigurationProvider<TMapInput, TMapOutput>));

        private readonly Dictionary<string, ContextAndServiceConfiguration> _configurationProvider;
        private readonly ISet<string> _failedEvaluators;
        private readonly ISet<string> _submittedEvaluators; 
        private readonly object _lock;
        private readonly int _numNodes;
        private int _assignedPartitionDescriptors;
        private readonly IGroupCommDriver _groupCommDriver;
        private readonly ConfigurationManager _configurationManager;
        private readonly Stack<IPartitionDescriptor> _partitionDescriptors;

        internal ServiceAndContextConfigurationProvider(int numNodes, IGroupCommDriver groupCommDriver,
            ConfigurationManager configurationManager, Stack<IPartitionDescriptor> partitionDescriptors)
        {
            _configurationProvider = new Dictionary<string, ContextAndServiceConfiguration>();
            _failedEvaluators = new HashSet<string>();
            _submittedEvaluators = new HashSet<string>();
            _numNodes = numNodes;
            _groupCommDriver = groupCommDriver;
            _configurationManager = configurationManager;
            _assignedPartitionDescriptors = 0;
            _partitionDescriptors = partitionDescriptors;
            _lock = new object();
        }

        /// <summary>
        /// Handles failed evaluator. Moves the id from 
        /// submitted evaluator to failed evaluator
        /// </summary>
        /// <param name="evaluatorId"></param>
        internal void EvaluatorFailed(string evaluatorId)
        {
            lock (_lock)
            {
                if (!_submittedEvaluators.Contains(evaluatorId))
                {
                    Utilities.Diagnostics.Exceptions.Throw(new Exception("Failed evaluator was never submitted"), Logger);
                }

                _failedEvaluators.Add(evaluatorId);
                _submittedEvaluators.Remove(evaluatorId);
            }
        }

        /// <summary>
        /// Gives context and service configuration for next evaluator either from failed 
        /// evaluator or new configuration
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        internal ContextAndServiceConfiguration GetNextConfiguration(string evaluatorId)
        {
            lock (_lock)
            {
                if (_submittedEvaluators.Contains(evaluatorId))
                {
                    Utilities.Diagnostics.Exceptions.Throw(new Exception("The evaluator is already submitted"), Logger);
                }

                if (_failedEvaluators.Count == 0 && _assignedPartitionDescriptors >= _numNodes)
                {
                    Utilities.Diagnostics.Exceptions.Throw(new Exception("No more configuration can be provided"), Logger);
                }

                // if some failed id exists return that configuration
                if (_failedEvaluators.Count != 0)
                {
                    string failedEvaluatorId = _failedEvaluators.First();
                    _failedEvaluators.Remove(failedEvaluatorId);
                    var config = _configurationProvider[failedEvaluatorId];
                    _configurationProvider.Remove(failedEvaluatorId);
                    _configurationProvider[evaluatorId] = config;
                }
                else
                {
                    _assignedPartitionDescriptors++;

                    if (_configurationProvider.ContainsKey(evaluatorId))
                    {
                        Utilities.Diagnostics.Exceptions.Throw(
                            new Exception(
                                "Evaluator Id already present in configuration cache, they have to be unique"),
                            Logger);
                    }

                    // Checks whether to put update task configuration or map task configuration
                    if (_assignedPartitionDescriptors == 1)
                    {
                        _configurationProvider[evaluatorId] = GetUpdateTaskContextAndServiceConfiguration();
                    }
                    else
                    {
                        _configurationProvider[evaluatorId] =
                            GetMapTaskContextAndServiceConfiguration(_partitionDescriptors.Pop());
                    }
                }

                _submittedEvaluators.Add(evaluatorId);
                return _configurationProvider[evaluatorId];
            }
        }

        private ContextAndServiceConfiguration GetMapTaskContextAndServiceConfiguration(IPartitionDescriptor partitionDescriptor)
        {
            var codecConfig =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(
                        StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Conf.Set(
                            StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Codec,
                            GenericType<MapInputWithControlMessageCodec<TMapInput>>.Class).Build(),
                        StreamingCodecConfigurationMinusMessage<TMapOutput>.Conf.Build(),
                        _configurationManager.MapInputCodecConfiguration)
                    .Build();

            var contextConf = _groupCommDriver.GetContextConfiguration();
            var serviceConf = Configurations.Merge(_groupCommDriver.GetServiceConfiguration(), codecConfig, partitionDescriptor.GetPartitionConfiguration());

            return new ContextAndServiceConfiguration(contextConf, serviceConf);
        }

        private ContextAndServiceConfiguration GetUpdateTaskContextAndServiceConfiguration()
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
                        })
                    .Build();

            var serviceConf = Configurations.Merge(_groupCommDriver.GetServiceConfiguration(), codecConfig);
            return new ContextAndServiceConfiguration(_groupCommDriver.GetContextConfiguration(), serviceConf);
        }
    }
}

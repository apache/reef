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
using System.Runtime.Serialization;
using Org.Apache.REEF.Common.Catalog;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    [DataContract]
    internal class AllocatedEvaluator : IAllocatedEvaluator
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AllocatedEvaluator));
        
        private readonly AvroConfigurationSerializer _serializer;

        private IEvaluatorDescriptor _evaluatorDescriptor;

        private readonly ISet<IConfigurationProvider> _configurationProviders;

        public AllocatedEvaluator(IAllocatedEvaluatorClr2Java clr2Java, ISet<IConfigurationProvider> configurationProviders)
        {
            _configurationProviders = configurationProviders;
            InstanceId = Guid.NewGuid().ToString("N");
            _serializer = new AvroConfigurationSerializer();
            Clr2Java = clr2Java;
            Id = Clr2Java.GetId();
            ProcessNewEvaluator();

            NameServerInfo = Clr2Java.GetNameServerInfo();
        }

        [DataMember]
        public string InstanceId { get; set; }

        public string Id { get; private set; }

        public string EvaluatorBatchId { get; set; }

        public EvaluatorType Type { get; set; }

        public string NameServerInfo { get; set; }

        [DataMember]
        private IAllocatedEvaluatorClr2Java Clr2Java { get; set; }

        public void SubmitTask(IConfiguration taskConfiguration)
        {
            var contextConfiguration =
                ContextConfiguration.ConfigurationModule.Set(ContextConfiguration.Identifier, "RootContext_" + this.Id)
                    .Build();
            SubmitContextAndTask(contextConfiguration, taskConfiguration);
        }
        public void SubmitContext(IConfiguration contextConfiguration)
        {
            LOGGER.Log(Level.Info, "AllocatedEvaluator::SubmitContext");
            string context = _serializer.ToString(contextConfiguration);
            LOGGER.Log(Level.Verbose, "serialized contextConfiguration: " + context);
            Clr2Java.SubmitContext(context);
        }

        public void SubmitContextAndTask(IConfiguration contextConfiguration, IConfiguration taskConfiguration)
        {
            LOGGER.Log(Level.Info, "AllocatedEvaluator::SubmitContextAndTask");

            // TODO: Change this to service configuration when REEF-289(https://issues.apache.org/jira/browse/REEF-289) is fixed.
            taskConfiguration = MergeWithConfigurationProviders(taskConfiguration);
            string context = _serializer.ToString(contextConfiguration);
            string task = _serializer.ToString(taskConfiguration);

            LOGGER.Log(Level.Verbose, "serialized contextConfiguration: " + context);
            LOGGER.Log(Level.Verbose, "serialized taskConfiguration: " + task);

            Clr2Java.SubmitContextAndTask(context, task);
        }

        public void SubmitContextAndService(IConfiguration contextConfiguration, IConfiguration serviceConfiguration)
        {
            LOGGER.Log(Level.Info, "AllocatedEvaluator::SubmitContextAndService");

            var serviceConf = MergeWithConfigurationProviders(serviceConfiguration);
            string context = _serializer.ToString(contextConfiguration);
            string service = _serializer.ToString(WrapServiceConfigAsString(serviceConf));

            LOGGER.Log(Level.Verbose, "serialized contextConfiguration: " + context);
            LOGGER.Log(Level.Verbose, "serialized serviceConfiguration: " + service);

            Clr2Java.SubmitContextAndService(context, service);
        }

        public void SubmitContextAndServiceAndTask(IConfiguration contextConfiguration, IConfiguration serviceConfiguration, IConfiguration taskConfiguration)
        {
            LOGGER.Log(Level.Info, "AllocatedEvaluator::SubmitContextAndServiceAndTask");

            var serviceConf = MergeWithConfigurationProviders(serviceConfiguration);
            string context = _serializer.ToString(contextConfiguration);
            string service = _serializer.ToString(WrapServiceConfigAsString(serviceConf));
            string task = _serializer.ToString(taskConfiguration);

            LOGGER.Log(Level.Verbose, "serialized contextConfiguration: " + context);
            LOGGER.Log(Level.Verbose, "serialized serviceConfiguration: " + service);
            LOGGER.Log(Level.Verbose, "serialized taskConfiguration: " + task);

            Clr2Java.SubmitContextAndServiceAndTask(context, service, task);
        }

        public IEvaluatorDescriptor GetEvaluatorDescriptor()
        {
            return _evaluatorDescriptor;
        }

        public void Dispose()
        {
            Clr2Java.Close();
        }

        public INodeDescriptor GetNodeDescriptor()
        {
            throw new NotImplementedException();
        }

        public void AddFile(string file)
        {
            throw new NotImplementedException();
        }

        public void AddLibrary(string file)
        {
            throw new NotImplementedException();
        }

        public void AddFileResource(string file)
        {
            throw new NotImplementedException();
        }

        private void ProcessNewEvaluator()
        {
            _evaluatorDescriptor = Clr2Java.GetEvaluatorDescriptor();
            lock (EvaluatorRequestor.Evaluators)
            {
                foreach (KeyValuePair<string, IEvaluatorDescriptor> pair in EvaluatorRequestor.Evaluators)
                {
                    if (pair.Value.Equals(_evaluatorDescriptor))
                    {
                        var key = pair.Key;
                        EvaluatorRequestor.Evaluators.Remove(key);
                        var assignedId = key.Substring(0, key.LastIndexOf(EvaluatorRequestor.BatchIdxSeparator));

                        LOGGER.Log(Level.Verbose, "Received evaluator [{0}] of memory {1}MB that matches request of {2}MB with batch id [{3}].", 
                            Id, _evaluatorDescriptor.Memory, pair.Value.Memory, assignedId);
                        EvaluatorBatchId = assignedId;
                        break;
                    }
                }
            }
        }

        private IConfiguration MergeWithConfigurationProviders(IConfiguration configuration)
        {
            IConfiguration config = configuration;
            if (_configurationProviders != null)
            {
                foreach (var configurationProvider in _configurationProviders)
                {
                    config = Configurations.Merge(config, configurationProvider.GetConfiguration());
                }
            }
            return config;
        }

        /// <summary>
        /// This is to wrap entire service configuration in to a serialized string
        /// At evaluator side, we will unwrap it to get the original Service Configuration string
        /// It is to avoid issues that some C# class Node are dropped in the deserialized ClassHierarchy by Goold ProtoBuffer deserializer
        /// </summary>
        /// <param name="serviceConfiguration"></param>
        /// <returns></returns>
        private IConfiguration WrapServiceConfigAsString(IConfiguration serviceConfiguration)
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<ServicesConfigurationOptions.ServiceConfigString, string>(
                    GenericType<ServicesConfigurationOptions.ServiceConfigString>.Class,
                    new AvroConfigurationSerializer().ToString(serviceConfiguration))
                .Build();
        }
    }
}

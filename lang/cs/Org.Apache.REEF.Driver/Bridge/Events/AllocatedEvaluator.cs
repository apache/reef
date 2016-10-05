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
using System.Runtime.Serialization;
using Org.Apache.REEF.Common.Catalog;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    [DataContract]
    internal class AllocatedEvaluator : IAllocatedEvaluator
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AllocatedEvaluator));
        
        private readonly AvroConfigurationSerializer _serializer;

        private IEvaluatorDescriptor _evaluatorDescriptor;

        private readonly string _evaluatorConfigStr;

        public AllocatedEvaluator(IAllocatedEvaluatorClr2Java clr2Java, ISet<IConfigurationProvider> configurationProviders)
        {
            _serializer = new AvroConfigurationSerializer();

            var evaluatorConfig = TangFactory.GetTang().NewConfigurationBuilder().Build();
            foreach (var configurationProvider in configurationProviders)
            {
                evaluatorConfig = Configurations.Merge(evaluatorConfig, configurationProvider.GetConfiguration());
            }

            _evaluatorConfigStr = _serializer.ToString(evaluatorConfig);

            Clr2Java = clr2Java;
            Id = Clr2Java.GetId();
            ProcessNewEvaluator();

            NameServerInfo = Clr2Java.GetNameServerInfo();
        }

        public string Id { get; private set; }

        public string EvaluatorBatchId { get; set; }

        public EvaluatorType Type { get; set; }

        public string NameServerInfo { get; set; }

        [DataMember]
        private IAllocatedEvaluatorClr2Java Clr2Java { get; set; }

        public void SubmitTask(IConfiguration taskConfiguration)
        {
            var contextConfiguration =
                Common.Context.ContextConfiguration.ConfigurationModule.Set(
                    Common.Context.ContextConfiguration.Identifier, "RootContext_" + this.Id).Build();

            Clr2Java.SubmitContextAndTask(_evaluatorConfigStr, _serializer.ToString(contextConfiguration), _serializer.ToString(taskConfiguration));
        }

        public void SubmitContext(IConfiguration contextConfiguration)
        {
            Clr2Java.SubmitContext(_evaluatorConfigStr, _serializer.ToString(contextConfiguration));
        }

        public void SubmitContextAndTask(IConfiguration contextConfiguration, IConfiguration taskConfiguration)
        {
            Clr2Java.SubmitContextAndTask(_evaluatorConfigStr, _serializer.ToString(contextConfiguration), _serializer.ToString(taskConfiguration));
        }

        public void SubmitContextAndService(IConfiguration contextConfiguration, IConfiguration serviceConfiguration)
        {
            Clr2Java.SubmitContextAndService(_evaluatorConfigStr, _serializer.ToString(contextConfiguration), _serializer.ToString(serviceConfiguration));
        }

        public void SubmitContextAndServiceAndTask(IConfiguration contextConfiguration, IConfiguration serviceConfiguration, IConfiguration taskConfiguration)
        {
            Clr2Java.SubmitContextAndServiceAndTask(
                _evaluatorConfigStr, _serializer.ToString(contextConfiguration), _serializer.ToString(serviceConfiguration), _serializer.ToString(taskConfiguration));
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
    }
}

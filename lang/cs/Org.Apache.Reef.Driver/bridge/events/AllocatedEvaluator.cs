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

using Org.Apache.Reef.Common.Catalog;
using Org.Apache.Reef.Common.Evaluator;
using Org.Apache.Reef.Driver.Evaluator;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Formats;
using Org.Apache.Reef.Tang.Interface;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Runtime.Serialization;

namespace Org.Apache.Reef.Driver.Bridge
{
    [DataContract]
    internal class AllocatedEvaluator : IAllocatedEvaluator
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AllocatedEvaluator));
        
        private readonly AvroConfigurationSerializer _serializer;

        private IEvaluatorDescriptor _evaluatorDescriptor;

        public AllocatedEvaluator(IAllocatedEvaluaotrClr2Java clr2Java)
        {
            InstanceId = Guid.NewGuid().ToString("N");
            _serializer = new AvroConfigurationSerializer();
            Clr2Java = clr2Java;
            Id = Clr2Java.GetId();
            ProcessNewEvaluator();

            NameServerInfo = Clr2Java.GetNameServerInfo();
        }

        [DataMember]
        public string InstanceId { get; set; }

        public string Id { get; set; }

        public string EvaluatorBatchId { get; set; }

        public EvaluatorType Type { get; set; }

        public string NameServerInfo { get; set; }

        [DataMember]
        private IAllocatedEvaluaotrClr2Java Clr2Java { get; set; }

        public void SubmitContext(IConfiguration contextConfiguration)
        {
            LOGGER.Log(Level.Info, "AllocatedEvaluator::SubmitContext");
            string context = _serializer.ToString(contextConfiguration);
            LOGGER.Log(Level.Info, "serialized contextConfiguration: " + context);
            Clr2Java.SubmitContext(context);
        }

        public void SubmitContextAndTask(IConfiguration contextConfiguration, IConfiguration taskConfiguration)
        {
            LOGGER.Log(Level.Info, "AllocatedEvaluator::SubmitContextAndTask");

            string context = _serializer.ToString(contextConfiguration);
            string task = _serializer.ToString(taskConfiguration);

            LOGGER.Log(Level.Info, "serialized contextConfiguration: " + context);
            LOGGER.Log(Level.Info, "serialized taskConfiguration: " + task);

            Clr2Java.SubmitContextAndTask(context, task);
        }

        public void SubmitContextAndService(IConfiguration contextConfiguration, IConfiguration serviceConfiguration)
        {
            LOGGER.Log(Level.Info, "AllocatedEvaluator::SubmitContextAndService");

            string context = _serializer.ToString(contextConfiguration);
            string service = _serializer.ToString(serviceConfiguration);

            LOGGER.Log(Level.Info, "serialized contextConfiguration: " + context);
            LOGGER.Log(Level.Info, "serialized serviceConfiguration: " + service);

            Clr2Java.SubmitContextAndService(context, service);
        }

        public void SubmitContextAndServiceAndTask(IConfiguration contextConfiguration, IConfiguration serviceConfiguration, IConfiguration taskConfiguration)
        {
            LOGGER.Log(Level.Info, "AllocatedEvaluator::SubmitContextAndServiceAndTask");

            string context = _serializer.ToString(contextConfiguration);
            string service = _serializer.ToString(serviceConfiguration);
            string task = _serializer.ToString(taskConfiguration);

            LOGGER.Log(Level.Info, "serialized contextConfiguration: " + context);
            LOGGER.Log(Level.Info, "serialized serviceConfiguration: " + service);
            LOGGER.Log(Level.Info, "serialized taskConfiguration: " + task);

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
                        string key = pair.Key;
                        EvaluatorRequestor.Evaluators.Remove(key);
                        string assignedId = key.Substring(0, key.LastIndexOf('_'));
                        string message = string.Format(
                            CultureInfo.InvariantCulture,
                            "Received evalautor [{0}] of memory {1}MB that matches request of {2}MB with batch id [{3}].",
                            Id,
                            _evaluatorDescriptor.Memory,
                            pair.Value.Memory,
                            assignedId);

                        LOGGER.Log(Level.Verbose, message);
                        EvaluatorBatchId = assignedId;
                        break;
                    }
                }
            }
        }
    }
}

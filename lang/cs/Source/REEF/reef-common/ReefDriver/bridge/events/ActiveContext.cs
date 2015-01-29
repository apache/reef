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

using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.Driver.Evaluator;
using Org.Apache.Reef.Utilities;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Formats;
using Org.Apache.Reef.Tang.Interface;
using System;
using System.Runtime.Serialization;

namespace Org.Apache.Reef.Driver.Bridge
{
    [DataContract]
    internal class ActiveContext : IActiveContext
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ActiveContext));

        private readonly AvroConfigurationSerializer _serializer;

        public ActiveContext(IActiveContextClr2Java clr2Java)
        {
            InstanceId = Guid.NewGuid().ToString("N");
            Clr2Java = clr2Java;
            _serializer = new AvroConfigurationSerializer();
        }

        [DataMember]
        public string InstanceId { get; set; }

        public string Id
        {
            get
            {
                return Clr2Java.GetId();
            }

            set
            {
            }
        }

        public string EvaluatorId
        {
            get
            {
                return Clr2Java.GetEvaluatorId();
            }

            set
            {
            }
        }

        public Optional<string> ParentId { get; set; }

        public IEvaluatorDescriptor EvaluatorDescriptor
        {
            get
            {
                return Clr2Java.GetEvaluatorDescriptor();
            }

            set
            {
            }
        }

        private IActiveContextClr2Java Clr2Java { get; set; }

        public void SubmitTask(IConfiguration taskConfiguration)
        {
            LOGGER.Log(Level.Info, "ActiveContext::SubmitTask");
            string task = _serializer.ToString(taskConfiguration);
            LOGGER.Log(Level.Info, "serialized taskConfiguration: " + task);
            Clr2Java.SubmitTask(task);
        }

        public void Dispose()
        {
            LOGGER.Log(Level.Info, "ActiveContext::Dispose");
            Clr2Java.Close();
        }

        public void SubmitContext(IConfiguration contextConfiguration)
        {
            throw new NotImplementedException();
        }

        public void SubmitContextAndService(IConfiguration contextConfiguration, IConfiguration serviceConfiguration)
        {
            throw new NotImplementedException();
        }

        public void SendMessage(byte[] message)
        {
            throw new NotImplementedException();
        }
    }
}

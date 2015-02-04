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
using Org.Apache.Reef.Utilities.Diagnostics;
using Org.Apache.Reef.Utilities.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.Serialization;

namespace Org.Apache.Reef.Driver.Bridge
{
    [DataContract]
    internal class EvaluatorRequestor : IEvaluatorRequestor
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(EvaluatorRequestor));

        private static Dictionary<string, IEvaluatorDescriptor> _evaluators;
        
        public EvaluatorRequestor(IEvaluatorRequestorClr2Java clr2Java)
        {
            InstanceId = Guid.NewGuid().ToString("N");
            Clr2Java = clr2Java;
        }

        public static Dictionary<string, IEvaluatorDescriptor> Evaluators
        {
            get
            {
                if (_evaluators == null)
                {
                    _evaluators = new Dictionary<string, IEvaluatorDescriptor>();
                }
                return _evaluators;
            }
        }

        public IResourceCatalog ResourceCatalog { get; set; }

        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        private IEvaluatorRequestorClr2Java Clr2Java { get; set; }

        public void Submit(IEvaluatorRequest request)
        {
            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Submitting request for {0} evaluators and {1} MB memory and  {2} core to rack {3}.", request.Number, request.MemoryMegaBytes, request.VirtualCore, request.Rack));

            lock (Evaluators)
            {
                for (int i = 0; i < request.Number; i++)
                {
                    EvaluatorDescriptorImpl descriptor = new EvaluatorDescriptorImpl(new NodeDescriptorImpl(), EvaluatorType.CLR, request.MemoryMegaBytes, request.VirtualCore);
                    descriptor.Rack = request.Rack;
                    string key = string.Format(CultureInfo.InvariantCulture, "{0}_{1}", request.EvaluatorBatchId, i);
                    try
                    {
                        _evaluators.Add(key, descriptor);
                    }
                    catch (ArgumentException e)
                    {
                        Exceptions.Caught(e, Level.Error, string.Format(CultureInfo.InvariantCulture, "EvaluatorBatchId [{0}] already exists.", key), LOGGER);
                        Exceptions.Throw(new InvalidOperationException("Cannot use evaluator id " + key, e), LOGGER);
                    }
                }
            }
            
            Clr2Java.Submit(request);
        }

        public void Dispose()
        {
        }
    }
}

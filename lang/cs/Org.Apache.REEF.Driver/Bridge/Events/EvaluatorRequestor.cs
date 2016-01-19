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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.Serialization;
using Org.Apache.REEF.Common.Catalog;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    [DataContract]
    internal sealed class EvaluatorRequestor : IEvaluatorRequestor
    {
        internal const char BatchIdxSeparator = '_';
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(EvaluatorRequestor));

        private static readonly IDictionary<string, IEvaluatorDescriptor> EvaluatorDescriptorsDictionary =
            new Dictionary<string, IEvaluatorDescriptor>();

        internal EvaluatorRequestor(IEvaluatorRequestorClr2Java clr2Java)
        {
            InstanceId = Guid.NewGuid().ToString("N");
            Clr2Java = clr2Java;
        }

        /// <summary>
        /// A map of EvaluatorBatchID + BatchIdxSeparator + (Evaluator number in the batch) to 
        /// the Evaluator descriptor for the Evaluator.
        /// </summary>
        internal static IDictionary<string, IEvaluatorDescriptor> Evaluators
        {
            get { return EvaluatorDescriptorsDictionary; }
        }

        public IResourceCatalog ResourceCatalog { get; set; }

        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        private IEvaluatorRequestorClr2Java Clr2Java { get; set; }

        public void Submit(IEvaluatorRequest request)
        {
            LOGGER.Log(Level.Info, "Submitting request for {0} evaluators and {1} MB memory and  {2} core to rack {3}.", request.Number, request.MemoryMegaBytes, request.VirtualCore, request.Rack);

            lock (Evaluators)
            {
                for (var i = 0; i < request.Number; i++)
                {
                    var descriptor = new EvaluatorDescriptorImpl(new NodeDescriptorImpl(), EvaluatorType.CLR, request.MemoryMegaBytes, request.VirtualCore, request.Rack);
                    var key = string.Format(CultureInfo.InvariantCulture, "{0}{1}{2}", request.EvaluatorBatchId, BatchIdxSeparator, i);
                    try
                    {
                        Evaluators.Add(key, descriptor);
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

        public EvaluatorRequestBuilder NewBuilder()
        {
            return new EvaluatorRequestBuilder();
        }

        public EvaluatorRequestBuilder NewBuilder(IEvaluatorRequest request)
        {
#pragma warning disable 618
            return new EvaluatorRequestBuilder(request);
#pragma warning restore 618
        }

        public void Dispose()
        {
        }
    }
}
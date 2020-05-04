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
using System.Runtime.Serialization;
using Org.Apache.REEF.Driver.Bridge.Events;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Evaluator
{
    /// <summary>
    /// Default implementation of IEvaluatorRequest.
    /// </summary>
    [DataContract]
    internal class EvaluatorRequest : IEvaluatorRequest
    {
        internal EvaluatorRequest()
            : this(number: 0, megaBytes: 0)

        private static readonly Logger Logger = Logger.GetLogger(typeof(EvaluatorRequest));

        internal EvaluatorRequest(int number, int megaBytes)
            : this(
                  number: number,
                  megaBytes: megaBytes,
                  core: 1)
        {
        }

        internal EvaluatorRequest(int number, int megaBytes, int core)
            : this(
                  number: number,
                  megaBytes: megaBytes,
                  core: core,
                  rack: string.Empty)
        {
        }

        internal EvaluatorRequest(int number, int megaBytes, string rack)
            : this(
                  number: number,
                  megaBytes: megaBytes,
                  core: 1,
                  rack: rack)
        {
        }

        internal EvaluatorRequest(int number, int megaBytes, int core, string rack)
            : this(
                  number: number,
                  megaBytes: megaBytes,
                  core: core,
                  rack: rack,
                  evaluatorBatchId: Guid.NewGuid().ToString("N"),
                  nodeNames: new string[] { })
        {
        }

        internal EvaluatorRequest(
            int number,
            int megaBytes,
            int core,
            string rack,
            string evaluatorBatchId,
            ICollection<string> nodeNames)
                : this(
                number: number,
                megaBytes: megaBytes,
                core: core,
                rack: rack,
                evaluatorBatchId: evaluatorBatchId,
                nodeNames: nodeNames,
                relaxLocality: true)
        {
        }

        internal EvaluatorRequest(
            int number,
            int megaBytes,
            int core,
            string rack,
            string evaluatorBatchId,
            ICollection<string> nodeNames,
            bool relaxLocality)
                : this(
                    requestId: string.Empty,
                    number: number,
                    megaBytes: megaBytes,
                    core: core,
                    rack: rack,
                    evaluatorBatchId: evaluatorBatchId,
                    runtimeName: string.Empty,
                    nodeNames: nodeNames,
                    relaxLocality: relaxLocality,
                    nodeLabelExpression: string.Empty)
        {
        }

        internal EvaluatorRequest(
            string requestId,
            int number,
            int megaBytes,
            int core,
            string rack,
            string evaluatorBatchId,
            string runtimeName,
            ICollection<string> nodeNames,
            bool relaxLocality,
            string nodeLabelExpression)
        {
            Logger.Log(Level.Verbose, "EvaluatorRequest constructor: RequestId {0}, Number: {1},  Priority: {2}.", requestId, number, priority);
            RequestId = requestId;
            Number = number;
            MemoryMegaBytes = megaBytes;
            VirtualCore = core;
            Priority = priority;
            Rack = rack;
            EvaluatorBatchId = evaluatorBatchId;
            RuntimeName = runtimeName;
            NodeNames = nodeNames;
            RelaxLocality = relaxLocality;
            NodeLabelExpression = nodeLabelExpression;
        }

        [DataMember]
        public string RequestId { get; private set; }

        [DataMember]
        public int MemoryMegaBytes { get; private set; }

        [DataMember]
        public int Number { get; private set; }

        [DataMember]
        public int VirtualCore { get; private set; }

        [DataMember]
        public string Rack { get; private set; }

        [DataMember]
        public string EvaluatorBatchId { get; private set; }

        [DataMember]
        public string RuntimeName { get; private set; }

        [DataMember]
        public ICollection<string> NodeNames { get; private set; }

        [DataMember]
        public bool RelaxLocality { get; private set; }

        [DataMember]
        public string NodeLabelExpression { get; private set; }

        internal static EvaluatorRequestBuilder NewBuilder()
        {
            return new EvaluatorRequestBuilder();
        }

        internal static EvaluatorRequestBuilder NewBuilder(EvaluatorRequest request)
        {
            return new EvaluatorRequestBuilder(request);
        }
    }
}
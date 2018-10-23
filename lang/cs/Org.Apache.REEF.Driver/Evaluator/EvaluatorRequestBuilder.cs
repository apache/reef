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
using Org.Apache.REEF.Common.Runtime;

namespace Org.Apache.REEF.Driver.Evaluator
{
    public sealed class EvaluatorRequestBuilder
    {
        private string _evaluatorBatchId;
        private string _rackName;
        private string _runtimeName;
        private ICollection<string> _nodeNames;
        private bool _relaxLocality;
        private string _nodeLabelExpression;

        internal EvaluatorRequestBuilder(IEvaluatorRequest request)
        {
            RequestId = request.RequestId;
            Number = request.Number;
            MegaBytes = request.MemoryMegaBytes;
            VirtualCore = request.VirtualCore;
            _evaluatorBatchId = request.EvaluatorBatchId;
            _rackName = request.Rack;
            _runtimeName = request.RuntimeName;
            _nodeNames = request.NodeNames;
            _relaxLocality = request.RelaxLocality;
            _nodeLabelExpression = request.NodeLabelExpression;
        }

        internal EvaluatorRequestBuilder()
        {
            RequestId = "RequestId-" + Guid.NewGuid().ToString("N");
            Number = 1;
            VirtualCore = 1;
            MegaBytes = 64;
            _rackName = string.Empty;
            _evaluatorBatchId = Guid.NewGuid().ToString("N");
            _runtimeName = string.Empty;
            _nodeNames = Enumerable.Empty<string>().ToList();
            _relaxLocality = true;
            _nodeLabelExpression = string.Empty;
        }

        public string RequestId { get; private set; }

        public int Number { get; private set; }
        public int MegaBytes { get; private set; }
        public int VirtualCore { get; private set; }

        /// <summary>
        /// Set the request id for the evaluator request.
        /// </summary>
        /// <param name="requestId"></param>
        /// <returns></returns>
        public EvaluatorRequestBuilder SetRequestId(string requestId)
        {
            RequestId = requestId;
            return this;
        }

        /// <summary>
        /// Set the number of evaluators to request.
        /// </summary>
        /// <param name="number"></param>
        /// <returns>this</returns>
        public EvaluatorRequestBuilder SetNumber(int number)
        {
            Number = number;
            return this;
        }

        /// <summary>
        /// Set the amount of memory (in MB) for the evaluator.
        /// </summary>
        /// <param name="megabytes"></param>
        /// <returns>this</returns>
        public EvaluatorRequestBuilder SetMegabytes(int megabytes)
        {
            MegaBytes = megabytes;
            return this;
        }

        /// <summary>
        /// Set the number of CPU cores for the evaluator.
        /// </summary>
        /// <param name="numberOfCores"></param>
        /// <returns>this</returns>
        public EvaluatorRequestBuilder SetCores(int numberOfCores)
        {
            VirtualCore = numberOfCores;
            return this;
        }

        /// <summary>
        /// Set the rack name to do the request for.
        /// </summary>
        /// <param name="rackName"></param>
        /// <returns>this</returns>
        public EvaluatorRequestBuilder SetRackName(string rackName)
        {
            _rackName = rackName;
            return this;
        }

        /// <summary>
        /// Set desired node names for the Evaluator to be allocated on.
        /// </summary>
        /// <param name="nodeNames"></param>
        /// <returns>this</returns>
        public EvaluatorRequestBuilder AddNodeNames(ICollection<string> nodeNames)
        {
            _nodeNames = nodeNames;
            return this;
        }

        /// <summary>
        /// Set a desired node name for evaluator to be allocated
        /// </summary>
        /// <param name="nodeName"></param>
        /// <returns></returns>
        public EvaluatorRequestBuilder AddNodeName(string nodeName)
        {
            _nodeNames.Add(nodeName);
            return this;
        }

        /// <summary>
        /// Sets the batch ID for requested evaluators in the same request. The batch of Evaluators requested in the
        /// same request will have the same Evaluator Batch ID.
        /// </summary>
        /// <param name="evaluatorBatchId">The batch ID for the Evaluator request.</param>
        public EvaluatorRequestBuilder SetEvaluatorBatchId(string evaluatorBatchId)
        {
            _evaluatorBatchId = evaluatorBatchId;
            return this;
        }

        /// <summary>
        /// Sets the runtime name for requested evaluators in the same request. The batch of Evaluators requested in the 
        /// same request will have the same runtime name.
        /// </summary>
        /// <param name="runtimeName">The runtime name for the Evaluator request.</param>
        public EvaluatorRequestBuilder SetRuntimeName(RuntimeName runtimeName)
        {
            _runtimeName = (runtimeName == RuntimeName.Default) ? string.Empty : runtimeName.ToString();
            return this;
        }

        /// <summary>
        /// Set the relax locality for requesting evaluator with specified node names
        /// </summary>
        /// <param name="relaxLocality">Locality relax flag.</param>
        /// <returns>this</returns>
        public EvaluatorRequestBuilder SetRelaxLocality(bool relaxLocality)
        {
            _relaxLocality = relaxLocality;
            return this;
        }

        /// <summary>
        /// Set the node label expression.
        /// </summary>
        /// <param name="nodeLabelExpression">describing a desired node type.</param>
        /// <returns></returns>
        public EvaluatorRequestBuilder SetNodeLabelExpression(string nodeLabelExpression)
        {
            _nodeLabelExpression = nodeLabelExpression;
            return this;
        }

        /// <summary>
        /// Build the EvaluatorRequest.
        /// </summary>
        /// <returns></returns>
        public IEvaluatorRequest Build()
        {
            return new EvaluatorRequest(RequestId, Number, MegaBytes, VirtualCore, rack: _rackName,
                evaluatorBatchId: _evaluatorBatchId, runtimeName: _runtimeName, nodeNames: _nodeNames,
                relaxLocality: _relaxLocality, nodeLabelExpression: _nodeLabelExpression);
        }
    }
}
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
using Org.Apache.REEF.Common.Runtime;

namespace Org.Apache.REEF.Driver.Evaluator
{
    public sealed class EvaluatorRequestBuilder
    {
        private string _evaluatorBatchId;
        private string _rackName;
        private string _runtimeName;

        internal EvaluatorRequestBuilder(IEvaluatorRequest request)
        {
            Number = request.Number;
            MegaBytes = request.MemoryMegaBytes;
            VirtualCore = request.VirtualCore;
            _evaluatorBatchId = request.EvaluatorBatchId;
            _rackName = request.Rack;
            _runtimeName = request.RuntimeName;
        }

        internal EvaluatorRequestBuilder()
        {
            Number = 1;
            VirtualCore = 1;
            MegaBytes = 64;
            _rackName = string.Empty;
            _evaluatorBatchId = Guid.NewGuid().ToString("N");
            _runtimeName = string.Empty;
        }

        public int Number { get; private set; }
        public int MegaBytes { get; private set; }
        public int VirtualCore { get; private set; }

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
        /// Build the EvaluatorRequest.
        /// </summary>
        /// <returns></returns>
        public IEvaluatorRequest Build()
        {
            return new EvaluatorRequest(Number, MegaBytes, VirtualCore, rack: _rackName, evaluatorBatchId: _evaluatorBatchId, runtimeName: _runtimeName);
        }
    }
}
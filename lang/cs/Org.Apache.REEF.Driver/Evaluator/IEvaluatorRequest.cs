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

namespace Org.Apache.REEF.Driver.Evaluator
{
    /// <summary>
    /// A request for an Evaluator allocation.
    /// </summary>
    public interface IEvaluatorRequest
    {
        /// <summary>
        /// Memory for the Evaluator in megabytes.
        /// </summary>
        int MemoryMegaBytes { get; }

        /// <summary>
        /// Number of Evaluators to allocate.
        /// </summary>
        int Number { get; }

        /// <summary>
        /// Number of cores in the Evaluator.
        /// </summary>
        int VirtualCore { get; }

        /// <summary>
        /// The desired rack name for the Evaluator to be allocated in.
        /// </summary>
        string Rack { get; }

        /// <summary>
        /// The batch ID for requested evaluators. Evaluators requested in the same batch
        /// will have the same Batch ID.
        /// </summary>
        string EvaluatorBatchId { get; }
    }
}
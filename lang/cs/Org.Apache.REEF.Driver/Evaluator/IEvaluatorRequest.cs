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

using System.Collections.Generic;

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
        /// The desired node names for the Evaluator to be allocated on.
        /// </summary>
        ICollection<string> NodeNames { get; }

        /// <summary>
        /// The batch ID for requested evaluators. Evaluators requested in the same batch
        /// will have the same Batch ID.
        /// </summary>
        string EvaluatorBatchId { get; }

        /// <summary>
        /// The name of the runtime to allocate the evaluator on
        /// </summary>
        string RuntimeName { get; }

        /// <summary>
        /// For a request at a network hierarchy level, set whether locality can be relaxed to that level and beyond.
        /// If the flag is off on a rack-level ResourceRequest, containers at that request's priority
        /// will not be assigned to nodes on that request's rack unless requests specifically for
        /// those nodes have also been submitted.
        /// If the flag is off on an ANY-level ResourceRequest, containers at that request's priority
        /// will only be assigned on racks for which specific requests have also been submitted.
        /// For example, to request a container strictly on a specific node, the corresponding rack-level
        /// and any-level requests should have locality relaxation set to false. Similarly,
        /// to request a container strictly on a specific rack,
        /// the corresponding any-level request should have locality relaxation set to false.
        /// </summary>
        bool RelaxLocality { get; }

        /// <summary>
        /// For specifying a node label expression that can be used by the resource manager
        /// to aquire certain container types.
        /// </summary>
        string NodeLabelExpression { get; }
    }
}
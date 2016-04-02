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

using Org.Apache.REEF.Common.Catalog;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Common.Runtime;

namespace Org.Apache.REEF.Driver.Evaluator
{
    /// <summary>
    /// Metadata about an Evaluator.
    /// </summary>
    public interface IEvaluatorDescriptor
    {
        /// <summary>
        /// NodeDescriptor of the node where this Evaluator is running.
        /// </summary>
        INodeDescriptor NodeDescriptor { get; }

        /// <summary>
        /// type of Evaluator.
        /// </summary>
        EvaluatorType EvaluatorType { get; }

        /// <summary>
        /// the amount of memory allocated to this Evaluator.
        /// </summary>
        int Memory { get; }

        /// <summary>
        /// the virtual core allocated to this Evaluator.
        /// </summary>
        int VirtualCore { get; }

        /// <summary>
        /// rack on which the evaluator was allocated
        /// </summary>
        string Rack { get; }

        /// <summary>
        /// name of the runtime that allocated this evaluator
        /// </summary>
        RuntimeName RuntimeName { get; }
    }
}
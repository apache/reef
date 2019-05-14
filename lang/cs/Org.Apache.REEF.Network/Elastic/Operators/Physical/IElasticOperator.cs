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

using Org.Apache.REEF.Network.Elastic.Task;
using Org.Apache.REEF.Utilities.Attributes;
using System;
using System.Threading;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical
{
    /// <summary>
    /// Base class for task-side, physical, group communication operators.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface IElasticOperator : IWaitForTaskRegistration, IReschedulable, IDisposable
    {
        /// <summary>
        /// The operator type.
        /// </summary>
        OperatorType OperatorType { get; }

        /// <summary>
        /// The operator identifier.
        /// </summary>
        int OperatorId { get; }

        /// <summary>
        /// Operator specific information in case of failure.
        /// </summary>
        string FailureInfo { get; }

        /// <summary>
        /// Get a reference of the iterator in the pipeline (if it exists).
        /// </summary>
        IElasticIterator IteratorReference { set; }

        /// <summary>
        /// Cancellation source for stopping the exeuction of the opearator.
        /// </summary>
        CancellationTokenSource CancellationSource { get; set; }

        /// <summary>
        /// Wait until computation is globally completed for this operator
        /// before disposing the object.
        /// </summary>
        void WaitCompletionBeforeDisposing();

        /// <summary>
        /// Reset the internal position tracker. This should be called
        /// every time a new iteration start in the workflow.
        /// </summary>
        void Reset();
    }
}
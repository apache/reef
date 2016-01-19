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
using System.Runtime.Serialization;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    /// <summary>
    /// A proxy for method calls to Java's ClosedContext.
    /// </summary>
    internal sealed class ClosedContext : IClosedContext
    {
        private readonly IActiveContextClr2Java _parentContextClr2Java;

        internal ClosedContext(IClosedContextClr2Java clr2java)
        {
            InstanceId = Guid.NewGuid().ToString("N");
            Id = clr2java.GetId();
            EvaluatorId = clr2java.GetEvaluatorId();
            EvaluatorDescriptor = clr2java.GetEvaluatorDescriptor();

            // TODO[JIRA REEF-762]: populate this
            _parentContextClr2Java = null;
        }

        /// <summary>
        /// An ID for the ClosedContext instance.
        /// </summary>
        [DataMember]
        public string InstanceId { get; private set; }

        /// <summary>
        /// Gets the ID of the closed context.
        /// </summary>
        public string Id { get; private set; }

        /// <summary>
        /// Gets the ID of the Evaluator on which the context was closed.
        /// </summary>
        public string EvaluatorId { get; private set; }

        /// <summary>
        /// Gets the ID of the parent context of the closed context.
        /// </summary>
        public Optional<string> ParentId
        {
            // TODO[REEF-762]: Implement
            get { return Optional<string>.Empty(); }
        }

        /// <summary>
        /// Gets the <see cref="IEvaluatorDescriptor"/> of the Evaluator on which
        /// the context was closed.
        /// </summary>
        public IEvaluatorDescriptor EvaluatorDescriptor { get; private set; }

        /// <summary>
        /// Gest the <see cref="IActiveContext"/> of the parent context of the
        /// closed context.
        /// </summary>
        public IActiveContext ParentContext
        {
            // TODO[JIRA REEF-762]: make sure this doesn't fail.
            get { return new ActiveContext(_parentContextClr2Java); }
        }
    }
}
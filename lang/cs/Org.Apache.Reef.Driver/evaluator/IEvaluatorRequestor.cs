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

using System.Collections.Generic;

using Org.Apache.Reef.Common.Catalog;

namespace Org.Apache.Reef.Driver.Evaluator
{
    /// <summary>
    /// Interface through which Evaluators can be requested.
    /// </summary>
    public interface IEvaluatorRequestor
    {
        /// <summary>
        /// Access to the {@link ResourceCatalog} for the cluster this Factory has access to
        /// </summary>
        IResourceCatalog ResourceCatalog { get; set; }

        /// <summary>
        /// Map between user evaluator id and evaluator information
        /// </summary>
        //IDictionary<string, IEvaluatorDescriptor> Evaluators { get; }

        /// <summary>
        /// Submit the request for new evaluator. The response will surface in the AllocatedEvaluator message handler.
        /// </summary>
        /// <param name="request"></param>
        void Submit(IEvaluatorRequest request);
    }
}

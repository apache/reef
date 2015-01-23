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

using Org.Apache.Reef.Common.Capabilities;
using Org.Apache.Reef.Common.Catalog;
using Org.Apache.Reef.Driver.Bridge;
using System.Collections.Generic;

namespace Org.Apache.Reef.Driver.Evaluator
{
    public class EvaluatorRequestBuilder
    {
        public EvaluatorRequestBuilder(EvaluatorRequest request)
        {
            foreach (ICapability capability in request.Capabilities)
            {
                Capabilities.Add(capability);
            }
            Number = request.Number;
            Catalog = request.Catalog;
            MegaBytes = request.MemoryMegaBytes;
            VirtualCore = request.VirtualCore;
        }

        internal EvaluatorRequestBuilder()
        {
        }

        public int Number { get; set; }

        public List<ICapability> Capabilities { get; set; }

        public IResourceCatalog Catalog { get; set; }

        public int MegaBytes { get; set; }

        public int VirtualCore { get; set; }

        public EvaluatorRequest Build()
        {
            return new EvaluatorRequest(Number, MegaBytes, VirtualCore, Capabilities, Catalog);
        }
    }
}

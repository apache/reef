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
using Org.Apache.Reef.Driver.Evaluator;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Org.Apache.Reef.Driver.Bridge
{
    [DataContract]
    public class EvaluatorRequest : IEvaluatorRequest
    {
        public EvaluatorRequest() 
            : this(0, 0, 1, string.Empty, Guid.NewGuid().ToString("N"))
        {
        }

        public EvaluatorRequest(int number, int megaBytes) 
            : this(number, megaBytes, 1, string.Empty, Guid.NewGuid().ToString("N"))
        {
        }

        public EvaluatorRequest(int number, int megaBytes, int core)
            : this(number, megaBytes, core, string.Empty, Guid.NewGuid().ToString("N"))
        {
        }

        public EvaluatorRequest(int number, int megaBytes, string rack)
            : this(number, megaBytes, 1, rack, Guid.NewGuid().ToString("N"))
        {
        }

        public EvaluatorRequest(int number, int megaBytes, int core, string rack)
            : this(number, megaBytes, core, rack, Guid.NewGuid().ToString("N"))
        {
        }

        public EvaluatorRequest(int number, int megaBytes, int core, string rack, string evaluatorBatchId)
        {
            Number = number;
            MemoryMegaBytes = megaBytes;
            VirtualCore = core;
            Rack = rack;
            EvaluatorBatchId = evaluatorBatchId;
        }

        public EvaluatorRequest(int number, int megaBytes, int core, List<ICapability> capabilitieses, IResourceCatalog catalog)
        {
            Number = number;
            MemoryMegaBytes = megaBytes;
            Capabilities = capabilitieses;
            VirtualCore = core;
            Catalog = catalog;
            EvaluatorBatchId = Guid.NewGuid().ToString("N");
        }

        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public int MemoryMegaBytes { get; set; }

        [DataMember]
        public int Number { get; set; }
        
        [DataMember]
        public int VirtualCore { get; set; }

        [DataMember]
        public string Rack { get; set; }

        [DataMember]
        public string EvaluatorBatchId { get; set; }

        public List<ICapability> Capabilities { get; set; }

        public IResourceCatalog Catalog { get; set; }

        public static EvaluatorRequestBuilder NewBuilder()
        {
            return new EvaluatorRequestBuilder();
        }

        public static EvaluatorRequestBuilder NewBuilder(EvaluatorRequest request)
        {
            return new EvaluatorRequestBuilder(request);
        }
    }
}

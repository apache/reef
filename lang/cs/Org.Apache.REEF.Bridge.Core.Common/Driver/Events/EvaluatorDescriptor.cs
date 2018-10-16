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
using Org.Apache.REEF.Driver.Evaluator;
using System;

namespace Org.Apache.REEF.Bridge.Core.Common.Driver.Events
{
    internal sealed class EvaluatorDescriptor : IEvaluatorDescriptor
    {
        public INodeDescriptor NodeDescriptor { get; }

        public EvaluatorType EvaluatorType => EvaluatorType.CLR;

        public int Memory { get; }
        public int VirtualCore { get; }

        public string Rack
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public RuntimeName RuntimeName { get; }

        public EvaluatorDescriptor(
            INodeDescriptor nodeDescriptor,
            int memory,
            int virtualCore,
            RuntimeName runtimeName = RuntimeName.Local)
        {
            NodeDescriptor = nodeDescriptor;
            Memory = memory;
            VirtualCore = virtualCore;
            RuntimeName = runtimeName;
        }
    }
}
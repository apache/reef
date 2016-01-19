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

using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    internal sealed class FailedContext : IFailedContext
    {
        private readonly string _evaluatorId;
        private readonly string _id;
        private readonly Optional<string> _parentId;

        internal FailedContext(IFailedContextClr2Java clr2Java)
        {
            _id = clr2Java.GetId();
            _evaluatorId = clr2Java.GetEvaluatorId();
            _parentId = string.IsNullOrEmpty(clr2Java.GetParentId())
                ? Optional<string>.Empty()
                : Optional<string>.Of(clr2Java.GetParentId());
            FailedContextClr2Java = clr2Java;
        }

        private IFailedContextClr2Java FailedContextClr2Java { get; set; }

        public string Id
        {
            get { return _id; }
        }

        public string EvaluatorId
        {
            get { return _evaluatorId; }
        }

        public Optional<string> ParentId
        {
            get { return _parentId; }
        }

        public IEvaluatorDescriptor EvaluatorDescriptor
        {
            get { return FailedContextClr2Java.GetEvaluatorDescriptor(); }
        }

        public Optional<IActiveContext> ParentContext
        {
            get
            {
                var context = FailedContextClr2Java.GetParentContext();
                if (context != null)
                {
                    return Optional<IActiveContext>.Of(new ActiveContext(context));
                }
                return Optional<IActiveContext>.Empty();
            }
        }
    }
}
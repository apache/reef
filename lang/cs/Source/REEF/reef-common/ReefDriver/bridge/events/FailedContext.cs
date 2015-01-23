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

using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.Driver.Evaluator;
using Org.Apache.Reef.Utilities;

namespace Org.Apache.Reef.Driver.Bridge
{
    public class FailedContext : IFailedContext
    {
        private string _id;

        private string _evaluatorId;

        private string _parentId;

        public FailedContext(IFailedContextClr2Java clr2Java)
        {
            _id = clr2Java.GetId();
            _evaluatorId = clr2Java.GetEvaluatorId();
            _parentId = clr2Java.GetParentId();
            FailedContextClr2Java = clr2Java;
        }

        public string Id
        {
            get
            {
                return _id;
            }

            set
            {
            }
        }

        public string EvaluatorId
        {
            get
            {
                return _evaluatorId;
            }

            set
            {
            }
        }

        public Optional<string> ParentId
        {
            get
            {
                return string.IsNullOrEmpty(_parentId) ? 
                    Optional<string>.Empty() : 
                    Optional<string>.Of(_parentId);
            }

            set
            {
            }
        }

        public IEvaluatorDescriptor EvaluatorDescriptor
        {
            get
            {
                return FailedContextClr2Java.GetEvaluatorDescriptor();
            }

            set
            {
            }
        }

        public Optional<IActiveContext> ParentContext
        {
            get
            {
                IActiveContextClr2Java context = FailedContextClr2Java.GetParentContext();
                if (context != null)
                {
                    return Optional<IActiveContext>.Of(new ActiveContext(context));
                }
                else
                {
                    return Optional<IActiveContext>.Empty();
                }
            }
        }

        private IFailedContextClr2Java FailedContextClr2Java { get; set; }
    }
}

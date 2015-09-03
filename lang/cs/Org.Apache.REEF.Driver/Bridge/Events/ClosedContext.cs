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

using System;
using System.Runtime.Serialization;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    public class ClosedContext : IClosedContext
    {
        private readonly string _id;

        private readonly string _evaluatorId;

        public ClosedContext(IClosedContextClr2Java clr2java)
        {
            InstanceId = Guid.NewGuid().ToString("N");
            _id = clr2java.GetId();
            _evaluatorId = clr2java.GetEvaluatorId();
        }

        [DataMember]
        public string InstanceId { get; set; }

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

        public Optional<string> ParentId { get; set; }

        public IEvaluatorDescriptor EvaluatorDescriptor
        {
            get
            {
                return ClosedContextClr2JavaClr2Java.GetEvaluatorDescriptor();
            }

            set
            {
            }
        }

        public IActiveContext ParentContext
        {
            get
            {
                return new ActiveContext(ParentContextClr2Java);
            }
        }

        private IActiveContextClr2Java ParentContextClr2Java { get; set; }

        private IClosedContextClr2Java ClosedContextClr2JavaClr2Java { get; set; }
    }
}

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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    [DataContract]
    internal sealed class FailedEvaluator : IFailedEvaluator
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(FailedEvaluator));
        private readonly string _id;
        private readonly IList<IFailedContext> _failedContexts;

        public FailedEvaluator(IFailedEvaluatorClr2Java clr2Java)
        {
            InstanceId = Guid.NewGuid().ToString("N");
            FailedEvaluatorClr2Java = clr2Java;
            _id = FailedEvaluatorClr2Java.GetId();
            _failedContexts = new List<IFailedContext>(
                FailedEvaluatorClr2Java.GetFailedContextsClr2Java().Select(clr2JavaFailedContext => 
                    new FailedContext(clr2JavaFailedContext)));
        }

        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        private IFailedEvaluatorClr2Java FailedEvaluatorClr2Java { get; set; }

        public string Id
        {
            get { return _id; }
        }

        public EvaluatorException EvaluatorException
        {
            get { return FailedEvaluatorClr2Java.GetException(); }
        }

        public IList<IFailedContext> FailedContexts
        {
            get { return _failedContexts; }
        }

        public Optional<IFailedTask> FailedTask
        {
            get 
            {
                if (FailedEvaluatorClr2Java.GetFailedTaskClr2Java() == null)
                {
                    return Optional<IFailedTask>.Empty();
                }

                return Optional<IFailedTask>.Of(new FailedTask(FailedEvaluatorClr2Java.GetFailedTaskClr2Java()));
            }
        }
    }
}
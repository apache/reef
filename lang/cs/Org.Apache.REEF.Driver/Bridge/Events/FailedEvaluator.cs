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
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    [DataContract]
    internal sealed class FailedEvaluator : IFailedEvaluator
    {
        private readonly string _id;
        private readonly IList<IFailedContext> _failedContexts;
        private readonly EvaluatorException _evaluatorException;

        public FailedEvaluator(IFailedEvaluatorClr2Java clr2Java)
        {
            FailedEvaluatorClr2Java = clr2Java;
            _id = FailedEvaluatorClr2Java.GetId();
            _failedContexts = new List<IFailedContext>(
                FailedEvaluatorClr2Java.GetFailedContextsClr2Java().Select(clr2JavaFailedContext => 
                    new FailedContext(clr2JavaFailedContext)));

            var errorBytes = FailedEvaluatorClr2Java.GetErrorBytes();
            if (errorBytes != null)
            {
                var formatter = new BinaryFormatter();
                using (var memStream = new MemoryStream(errorBytes))
                {
                    var inner = (Exception)formatter.Deserialize(memStream);
                    _evaluatorException = new EvaluatorException(_id, inner.Message, inner);
                }
            }
            else
            {
                _evaluatorException = new EvaluatorException(
                    _id, FailedEvaluatorClr2Java.GetJavaCause(), FailedEvaluatorClr2Java.GetJavaStackTrace());
            }
        }

        [DataMember]
        private IFailedEvaluatorClr2Java FailedEvaluatorClr2Java { get; set; }

        public string Id
        {
            get { return _id; }
        }

        public EvaluatorException EvaluatorException
        {
            get { return _evaluatorException; }
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
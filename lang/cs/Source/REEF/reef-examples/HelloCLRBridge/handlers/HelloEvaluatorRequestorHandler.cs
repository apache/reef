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

using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Driver.Evaluator;
using System;

using Org.Apache.Reef.Tang.Annotations;

namespace Org.Apache.Reef.Examples.HelloCLRBridge
{
    public class HelloEvaluatorRequestorHandler : IObserver<IEvaluatorRequestor>
    {
        [Inject]
        public HelloEvaluatorRequestorHandler()
        {
        }

        public void OnNext(IEvaluatorRequestor evalutorRequestor)
        {
            int evaluatorsNumber = 1;
            int memory = 512;
            int core = 2;
            string rack = "WonderlandRack";
            string evaluatorBatchId = "evaluatorThatRequires512MBofMemory";
            EvaluatorRequest request = new EvaluatorRequest(evaluatorsNumber, memory, core, rack, evaluatorBatchId);

            evalutorRequestor.Submit(request);

            evaluatorsNumber = 1;
            memory = 1999;
            core = 2;
            rack = "WonderlandRack";
            evaluatorBatchId = "evaluatorThatRequires1999MBofMemory";
            request = new EvaluatorRequest(evaluatorsNumber, memory, core, rack, evaluatorBatchId);
            evalutorRequestor.Submit(request);
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }
    }
}

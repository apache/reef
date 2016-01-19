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

using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.AllHandlers
{
    /// <summary>
    /// A sample implementation of driver start handler
    /// </summary>
    public sealed class HelloDriverStartHandler : IObserver<IDriverStarted>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(HelloDriverStartHandler));
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        [Inject]
        private HelloDriverStartHandler(IEvaluatorRequestor evaluatorRequestor)
        {
            _evaluatorRequestor = evaluatorRequestor;
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }

        /// <summary>
        /// Called to start the user mode driver
        /// Sample code to create EvaluatorRequest and submit it
        /// </summary>
        /// <param name="driverStarted"></param>
        public void OnNext(IDriverStarted driverStarted)
        {
            Logger.Log(Level.Info, string.Format("HelloDriver started at {0}", driverStarted.StartTime));

            int evaluatorsNumber = 1;
            int memory = 512;
            int core = 2;
            string rack = "WonderlandRack";
            string evaluatorBatchId = "evaluatorThatRequires512MBofMemory";
            var request =
                _evaluatorRequestor.NewBuilder()
                    .SetNumber(evaluatorsNumber)
                    .SetMegabytes(memory)
                    .SetCores(core)
                    .SetRackName(rack)
                    .SetEvaluatorBatchId(evaluatorBatchId)
                    .Build();

            _evaluatorRequestor.Submit(request);

            evaluatorsNumber = 1;
            memory = 1999;
            core = 2;
            rack = "WonderlandRack";
            evaluatorBatchId = "evaluatorThatRequires1999MBofMemory";
            request =
                _evaluatorRequestor.NewBuilder()
                    .SetNumber(evaluatorsNumber)
                    .SetMegabytes(memory)
                    .SetCores(core)
                    .SetRackName(rack)
                    .SetEvaluatorBatchId(evaluatorBatchId)
                    .Build();
            _evaluatorRequestor.Submit(request);
        }
    }
}
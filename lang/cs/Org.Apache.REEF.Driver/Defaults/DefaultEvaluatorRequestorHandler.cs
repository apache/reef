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
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Defaults
{
    /// <summary>
    /// Default handler for close messages from the client: logging it
    /// </summary>
    [Obsolete("In Version 0.13. Have an instance of IEvaluatorRequestor injected instead.")]
    public class DefaultEvaluatorRequestorHandler : IObserver<IEvaluatorRequestor>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultClientCloseHandler));

        [Inject]
        public DefaultEvaluatorRequestorHandler()
        {
        }

        public void OnNext(IEvaluatorRequestor value)
        {
            LOGGER.Log(Level.Info, "Default evaluator requestor: requesting 1 evaluator with 512 MB");
            int evaluatorsNumber = 1;
            int memory = 512;
            string rack = "WonderlandRack";
            EvaluatorRequest request = new EvaluatorRequest(evaluatorsNumber, memory, rack);

            value.Submit(request);
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}
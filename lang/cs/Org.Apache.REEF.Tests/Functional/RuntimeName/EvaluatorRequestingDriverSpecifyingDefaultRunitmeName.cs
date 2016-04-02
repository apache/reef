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
using System.Globalization;
using Org.Apache.REEF.Common.Runtime;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.Driver
{
    public sealed class EvaluatorRequestingDriverSpecifyingDefaultRunitmeName : 
        IObserver<IDriverStarted>, 
        IObserver<IAllocatedEvaluator>,        
        IObserver<IRunningTask>         
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(EvaluatorRequestingDriver));

        private readonly IEvaluatorRequestor _evaluatorRequestor;

        [Inject]
        public EvaluatorRequestingDriverSpecifyingDefaultRunitmeName(IEvaluatorRequestor evaluatorRequestor)
        {
            _evaluatorRequestor = evaluatorRequestor;
        }

        public void OnNext(IDriverStarted value)
        {
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "ondriver.start {0}", value.StartTime));
            var request =
                _evaluatorRequestor.NewBuilder()
                    .SetNumber(1)
                    .SetMegabytes(512)
                    .SetCores(2)
                    .SetRackName("WonderlandRack")
                    .SetEvaluatorBatchId("TestEvaluator")
                    .SetRuntimeName(RuntimeName.Default)
                    .Build();
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "submitting evaluator request"));
            _evaluatorRequestor.Submit(request);
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "evaluator request submitted"));
        }

        public void OnNext(IAllocatedEvaluator eval)
        {
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Received evaluator. Runtime Name: {0}.", eval.GetEvaluatorDescriptor().RuntimeName));
            eval.Dispose();
        }

        public void OnNext(IRunningTask runningTask)
        {
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Received runing task. Runtime Name: {0}", runningTask.ActiveContext.EvaluatorDescriptor.RuntimeName));
        }

        public void OnError(Exception error)
        {
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "On error: {0}", error));
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}
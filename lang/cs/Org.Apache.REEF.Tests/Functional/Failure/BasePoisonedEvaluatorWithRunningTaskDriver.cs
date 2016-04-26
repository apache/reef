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

namespace Org.Apache.REEF.Tests.Functional.Failure
{
    /// <summary>
    /// Base class used by poisoning tests in which evaluator has running task.
    /// In case of evaluator failure, we expect to get Failed evaluator event, with attached information about failed task and context.
    /// This driver implements this check.
    /// </summary>
    internal class BasePoisonedEvaluatorWithRunningTaskDriver :
        BasePoisonedEvaluatorDriver,
        IObserver<IFailedEvaluator>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(BasePoisonedEvaluatorWithRunningTaskDriver));
        internal const string FailedEvaluatorMessage = "I have seen a failed evaluator with correct failed context and task.";

        [Inject]
        internal BasePoisonedEvaluatorWithRunningTaskDriver(IEvaluatorRequestor requestor) : base(requestor)
        {
        }

        public void OnNext(IFailedEvaluator value)
        {
            if (value.FailedTask.Value == null || !value.FailedTask.IsPresent())
            {
                throw new Exception("No failed Task associated with failed Evaluator.");
            }

            if (value.FailedTask.Value.Id != TaskId)
            {
                throw new Exception("Failed Task ID returned " + value.FailedTask.Value.Id
                    + ", was expecting Task ID " + TaskId);
            }

            var expectedStr = "expected a single Context with Context ID " + ContextId + ".";

            if (value.FailedContexts == null)
            {
                throw new Exception("No Context was present but " + expectedStr);
            }

            if (value.FailedContexts.Count != 1)
            {
                throw new Exception("Collection of failed Contexts contains " + value.FailedContexts.Count + " failed Contexts but " + expectedStr);
            }

            if (value.FailedContexts[0].Id != ContextId)
            {
                throw new Exception("Failed Context ID " + value.FailedContexts[0].Id + ", expected " + ContextId + ".");
            }

            // this log line is used for test success validation
            Logger.Log(Level.Info, FailedEvaluatorMessage);
        }
    }
}

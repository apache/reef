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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Tests.Functional.Failure
{
    /// <summary>
    /// Base class used by evaluator poisoning tests.
    /// In case of evaluator failure, we expect to NOT get Failed/Closed context or Failed/Completed task.
    /// This driver ensures that in case of any of these events an exception is thrown.
    /// Also, this driver abstracts the common IDriverStarted handler which submits evaluator request.
    /// </summary>
    internal class BasePoisonedEvaluatorDriver :
        IObserver<IDriverStarted>,
        IObserver<IAllocatedEvaluator>,
        IObserver<IClosedContext>,
        IObserver<IFailedContext>,
        IObserver<ICompletedTask>,
        IObserver<IFailedTask>
    {
        public static readonly string UnexpectedFailedContext = "A failed context was not expected.";
        public static readonly string UnexpectedClosedContext = "A closed context was not expected.";
        public static readonly string UnexpectedFailedTask = "A failed task was not expected.";
        public static readonly string UnexpectedCompletedTask = "A completed task was not expected.";

        private readonly IEvaluatorRequestor _requestor;
        protected readonly string ContextId;
        protected readonly string TaskId;

        [Inject]
        protected BasePoisonedEvaluatorDriver(IEvaluatorRequestor requestor)
        {
            _requestor = requestor;
            ContextId = Guid.NewGuid().ToString("N").Substring(0, 8);
            TaskId = Guid.NewGuid().ToString("N").Substring(0, 8);
        }

        public void OnNext(IDriverStarted value)
        {
            _requestor.Submit(_requestor.NewBuilder().Build());
        }

        public virtual void OnNext(IAllocatedEvaluator value)
        {
            value.SubmitContext(ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, ContextId)
                .Build());
        }

        public void OnNext(IFailedContext value)
        {
            throw new Exception(UnexpectedFailedContext);
        }

        public void OnNext(IClosedContext value)
        {
            throw new Exception(UnexpectedClosedContext);
        }

        public void OnNext(IFailedTask value)
        {
            throw new Exception(UnexpectedFailedTask);
        }

        public virtual void OnNext(ICompletedTask value)
        {
            throw new Exception(UnexpectedCompletedTask);
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

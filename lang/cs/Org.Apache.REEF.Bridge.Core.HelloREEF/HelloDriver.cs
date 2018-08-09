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

using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Time;
using System;

namespace Org.Apache.REEF.Bridge.Core.HelloREEF
{
    /// <summary>
    /// The Driver for HelloREEF: It requests a single Evaluator and then submits the HelloTask to it.
    /// </summary>
    public sealed class HelloDriver :
        IObserver<IAllocatedEvaluator>,
        IObserver<IFailedEvaluator>,
        IObserver<ICompletedEvaluator>,
        IObserver<IDriverStarted>,
        IObserver<IDriverStopped>,
        IObserver<IRunningTask>,
        IObserver<IActiveContext>,
        IObserver<ICompletedTask>,
        IObserver<IFailedTask>
    {
        private static readonly Logger _Logger = Logger.GetLogger(typeof(HelloDriver));
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        private readonly IClock _clock;

        [Inject]
        private HelloDriver(IClock clock, IEvaluatorRequestor evaluatorRequestor)
        {
            _clock = clock;
            _evaluatorRequestor = evaluatorRequestor;
        }

        /// <summary>
        /// Submits the HelloTask to the Evaluator.
        /// </summary>
        /// <param name="allocatedEvaluator">Newly allocated evaluator's proxy object.</param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            _Logger.Log(Level.Info, "Evaluator allocated: {0}", allocatedEvaluator);

            var taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "HelloTask")
                .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                .Build();

            _Logger.Log(Level.Verbose, "Submit task: {0}", taskConfiguration);
            allocatedEvaluator.SubmitTask(taskConfiguration);
        }

        public void OnNext(IFailedEvaluator value)
        {
            _Logger.Log(Level.Info, "Failed Evaluator: {0}", value.Id);
            throw value.EvaluatorException;
        }

        public void OnNext(ICompletedEvaluator value)
        {
            _Logger.Log(Level.Info, "Completed Evaluator: {0}", value.Id);
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }

        public void OnNext(IDriverStopped value)
        {
            _Logger.Log(Level.Info, "HelloDriver stopped at {0}", value.StopTime);
        }

        /// <summary>
        /// Called to start the user mode driver.
        /// </summary>
        /// <param name="driverStarted">Notification that the Driver is up and running.</param>
        public void OnNext(IDriverStarted driverStarted)
        {
            _Logger.Log(Level.Info, "HelloDriver started at {0}", driverStarted.StartTime);
            _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder().SetMegabytes(64).Build());
        }

        public void OnNext(IRunningTask value)
        {
            _Logger.Log(Level.Info, "HelloDriver received running task {0}", value.Id);
        }

        public void OnNext(ICompletedTask value)
        {
            _Logger.Log(Level.Info, "HelloDriver received completed task {0}", value.Id);
            var taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "FailedTask")
                .Set(TaskConfiguration.Task, GenericType<FailedTask>.Class)
                .Build();
            value.ActiveContext.SubmitTask(taskConfiguration);
        }

        public void OnNext(IFailedTask value)
        {
            _Logger.Log(Level.Info, "HelloDriver received failed task {0} with active context {0}", new object[] { value.Id, value.GetActiveContext().Value.Id });
            value.GetActiveContext().Value.Dispose();
            _Logger.Log(Level.Info, "HelloDriver closed active context {0}", value.GetActiveContext().Value.Id);
        }

        public void OnNext(IActiveContext value)
        {
            _Logger.Log(Level.Info, "HelloDriver received active context {0}", value.Id);
        }
    }
}
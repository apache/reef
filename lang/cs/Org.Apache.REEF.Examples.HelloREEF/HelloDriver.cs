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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.HelloREEF
{
    /// <summary>
    /// The Driver for HelloREEF: It requests a single Evaluator and then submits the HelloTask to it.
    /// </summary>
    public sealed class HelloDriver : IObserver<IAllocatedEvaluator>, IObserver<IDriverStarted>
    {
        private static readonly Logger _Logger = Logger.GetLogger(typeof(HelloDriver));
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        [Inject]
        private HelloDriver(IEvaluatorRequestor evaluatorRequestor)
        {
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

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
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
    }
}
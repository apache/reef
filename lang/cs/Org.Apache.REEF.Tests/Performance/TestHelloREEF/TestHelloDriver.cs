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
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Performance.TestHelloREEF
{
    /// <summary>
    /// The Driver for HelloREEF: It requests a single Evaluator and then submits the HelloTask to it.
    /// </summary>
    public sealed class TestHelloDriver : IObserver<IAllocatedEvaluator>,
        IObserver<IDriverStarted>,
        IObserver<IFailedEvaluator>,
        IObserver<IFailedTask>,
        IObserver<ICompletedTask>,
        IObserver<IRunningTask>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestHelloDriver));
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        private const string RequestIdPrefix = "RequestId-";

        /// <summary>
        /// Specify if the desired node names is relaxed
        /// </summary>
        private readonly bool _relaxLocality;

        private readonly int _numberOfContainers;

        /// <summary>
        /// Constructor of the driver
        /// </summary>
        /// <param name="evaluatorRequestor">Evaluator Requestor</param>
        /// <param name="relaxLocality">Relax indicator of evaluator node request</param>
        /// <param name="numberOfContainers">Relax indicator of evaluator node request</param>
        [Inject]
        private TestHelloDriver(IEvaluatorRequestor evaluatorRequestor,
            [Parameter(typeof(RelaxLocality))] bool relaxLocality,
            [Parameter(typeof(NumberOfContainers))] int numberOfContainers)
        {
            Logger.Log(Level.Info, "HelloDriverYarn Driver: numberOfContainers: {0}.", numberOfContainers);
            _evaluatorRequestor = evaluatorRequestor;
            _relaxLocality = relaxLocality;
            _numberOfContainers = numberOfContainers;
        }

        /// <summary>
        /// Submits the HelloTask to the Evaluator.
        /// </summary>
        /// <param name="allocatedEvaluator"></param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            var msg = string.Format("Received allocatedEvaluator-HostName: {0}, id {1}",
                allocatedEvaluator.GetEvaluatorDescriptor().NodeDescriptor.HostName,
                allocatedEvaluator.Id);
            using (Logger.LogFunction("IAllocatedEvaluator handler:", msg))
            {
                var taskConfiguration = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, "HelloTask-" + allocatedEvaluator.Id)
                    .Set(TaskConfiguration.Task, GenericType<TestHelloTask>.Class)
                    .Build();
                allocatedEvaluator.SubmitTask(taskConfiguration);
            }
        }

        /// <summary>
        /// Called to start the user mode driver
        /// </summary>
        /// <param name="driverStarted"></param>
        public void OnNext(IDriverStarted driverStarted)
        {
            Logger.Log(Level.Info, "Received IDriverStarted, numberOfContainers: {0}", _numberOfContainers);

            var requestId = RequestIdPrefix + Guid.NewGuid();
            _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder()
                .SetRequestId(requestId)
                .SetMegabytes(64)
                .SetNumber(_numberOfContainers)
                .SetRelaxLocality(_relaxLocality)
                .SetCores(1)
                .Build());
        }

        /// <summary>
        /// A simple ICompletedTask handler.
        /// </summary>
        /// <param name="value"></param>
        void IObserver<ICompletedTask>.OnNext(ICompletedTask value)
        {
            Logger.Log(Level.Info, "Received ICompletedTask: {0} with evaluator id: {1}.", value.Id, value.ActiveContext.EvaluatorId);
            value.ActiveContext.Dispose();
        }

        /// <summary>
        /// A simple IFailedTask handler.
        /// </summary>
        /// <param name="value"></param>
        void IObserver<IFailedTask>.OnNext(IFailedTask value)
        {
            Logger.Log(Level.Info, "Received IFailedTask: {0} with evaluator id: {1}.", value.Id, value.GetActiveContext().Value.EvaluatorId);
            value.GetActiveContext().Value.Dispose();
        }

        /// <summary>
        /// A simple IFailedEvaluator handler.
        /// </summary>
        /// <param name="value"></param>
        void IObserver<IFailedEvaluator>.OnNext(IFailedEvaluator value)
        {
            Logger.Log(Level.Info, "Received IFailedEvaluator: {0}.", value.Id);
        }

        /// <summary>
        /// A simple IRunningTask handler.
        /// </summary>
        /// <param name="value"></param>
        void IObserver<IRunningTask>.OnNext(IRunningTask value)
        {
            Logger.Log(Level.Info, "Received IRunningTask: {0} with evaluator id: {1}", value.Id, value.ActiveContext.EvaluatorId);
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }
    }

    [NamedParameter(documentation: "RelaxLocality for specifying evaluator node names", shortName: "RelaxLocality", defaultValue: "true")]
    internal class RelaxLocality : Name<bool>
    {
    }

    [NamedParameter(documentation: "NumberOfContainers", defaultValue: "1")]
    internal class NumberOfContainers : Name<int>
    {
    }
}

﻿// Licensed to the Apache Software Foundation (ASF) under one
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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Telemetry;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional.Messaging;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.Telemetry
{
    /// <summary>
    /// Test driver to test metrics
    /// </summary>
    class MetricsDriver :
        IObserver<IDriverStarted>,
        IObserver<IAllocatedEvaluator>,
        IObserver<IActiveContext>,
        IObserver<ICompletedTask>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MessageDriver));
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        /// <summary>
        /// a set of driver metrics observers.
        /// </summary>
        private readonly ISet<IObserver<IDriverMetrics>> _driverMetricsObservers;

        /// <summary>
        /// Hold data of driver metrics
        /// </summary>
        private readonly IDriverMetrics _driverMetrics;

        /// <summary>
        /// This driver inject DriverMetricsObservers and IDriverMetrics.
        /// It keeps updating the driver metrics when receiving events. 
        /// </summary>
        /// <param name="evaluatorRequestor"></param>
        /// <param name="driverMetricsObservers"></param>
        /// <param name="driverMetrics"></param>
        [Inject]
        public MetricsDriver(IEvaluatorRequestor evaluatorRequestor,
            [Parameter(typeof(DriverMetricsObservers))] ISet<IObserver<IDriverMetrics>> driverMetricsObservers,
            IDriverMetrics driverMetrics)
        {
            _evaluatorRequestor = evaluatorRequestor;
            _driverMetricsObservers = driverMetricsObservers;
            _driverMetrics = driverMetrics;
        }

        public void OnNext(IDriverStarted value)
        {
            UpdateMetrics(TestSystemState.TestSystemStateDriverStartedReceived);

            var request =
                _evaluatorRequestor.NewBuilder()
                    .SetNumber(1)
                    .SetMegabytes(512)
                    .SetCores(2)
                    .SetRackName("WonderlandRack")
                    .SetEvaluatorBatchId("MetricsEvaluator")
                    .Build();
            _evaluatorRequestor.Submit(request);
        }

        public void OnNext(IAllocatedEvaluator value)
        {
            Logger.Log(Level.Info, "Received IAllocatedEvaluator");
            UpdateMetrics(TestSystemState.TestSystemStateAllocatedEvaluatorReceived);

            const string contextId = "ContextID";
            var serviceConfiguration = ServiceConfiguration.ConfigurationModule
                .Build();

            var contextConfiguration1 = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, contextId)
                .Build();

            var contextConfiguration2 = MessageSenderConfigurationModule.ConfigurationModule.Build();

            var contextConfiguration = Configurations.Merge(contextConfiguration1, contextConfiguration2);
            value.SubmitContextAndService(contextConfiguration, serviceConfiguration);
        }

        public void OnNext(IActiveContext activeContext)
        {
            Logger.Log(Level.Info, "Received IActiveContext");
            UpdateMetrics(TestSystemState.TestSystemStateActiveContextReceived);

            const string taskId = "TaskID";
            var taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, taskId)
                .Set(TaskConfiguration.Task, GenericType<MetricsTask>.Class)
                .Build();
            activeContext.SubmitTask(taskConfiguration);
        }

        public void OnNext(ICompletedTask value)
        {
            Logger.Log(Level.Info, "Received ICompletedTask");
            UpdateMetrics(TestSystemState.TestSystemStateCompletedTaskReceived);

            value.ActiveContext.Dispose();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Call metrics observers with driver metrics data
        /// </summary>
        private void UpdateMetrics(TestSystemState systemState)
        {
            _driverMetrics.SystemState = systemState.ToString();
            _driverMetrics.TimeUpdated = DateTime.Now;

            foreach (var metricsObserver in _driverMetricsObservers)
            {
                metricsObserver.OnNext(_driverMetrics);
            }
        }
    }

    internal enum TestSystemState
    {
        TestSystemStateDriverStartedReceived,
        TestSystemStateAllocatedEvaluatorReceived,
        TestSystemStateActiveContextReceived,
        TestSystemStateCompletedTaskReceived
    }
}
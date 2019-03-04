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
using System.Threading;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Config;
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Task
{
    /// <summary>
    /// Default implementation of the task-side stage.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class DefaultElasticStage : IElasticStage
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(DefaultElasticStage));

        private readonly CancellationSource _cancellationSource;

        private readonly object _disposeLock = new object();
        private bool _disposed = false;

        /// <summary>
        /// Injectable constructor.
        /// </summary>
        [Inject]
        private DefaultElasticStage(
           [Parameter(typeof(OperatorParameters.StageName))] string stageName,
           [Parameter(typeof(OperatorParameters.SerializedOperatorConfigs))] IList<string> operatorConfigs,
           [Parameter(typeof(OperatorParameters.StartIteration))] int startIteration,
           AvroConfigurationSerializer configSerializer,
           Workflow workflow,
           DefaultCommunicationLayer commLayer,
           CancellationSource cancellationSource,
           IInjector injector)
        {
            StageName = stageName;
            Workflow = workflow;
     
            _cancellationSource = cancellationSource;

            foreach (string operatorConfigStr in operatorConfigs)
            {
                IConfiguration operatorConfig = configSerializer.FromString(operatorConfigStr);
                IInjector operatorInjector = injector.ForkInjector(operatorConfig);
                string msgType = operatorInjector.GetNamedInstance<OperatorParameters.MessageType, string>();
                Type groupCommOperatorGenericInterface = typeof(IElasticTypedOperator<>);
                Type groupCommOperatorInterface =
                    groupCommOperatorGenericInterface.MakeGenericType(Type.GetType(msgType));
                var operatorObj = operatorInjector.GetInstance(groupCommOperatorInterface);

                Workflow.Add((IElasticOperator)operatorObj);
            }
        }

        /// <summary>
        /// The stage name.
        /// </summary>
        public string StageName { get; private set; }

        /// <summary>
        /// The workflow of the stage.
        /// </summary>
        public Workflow Workflow { get; private set; }

        /// <summary>
        /// Initializes the communication group.
        /// Computation blocks until all required tasks are registered in the group.
        /// </summary>
        /// <param name="cancellationSource">The signal to cancel the operation</param>
        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource = null)
        {
            try
            {
                Workflow.WaitForTaskRegistration(cancellationSource ?? _cancellationSource.Source);
            }
            catch (OperationCanceledException e)
            {
                Log.Log(Level.Error, "Stage " + StageName + " failed during registration.", e);
                throw e;
            }
        }

        /// <summary>
        /// Dispose the stage.
        /// </summary>
        public void Dispose()
        {
            lock (_disposeLock)
            {
                if (!_disposed)
                {
                    Workflow?.Dispose();

                    _disposed = true;
                }
            }
        }

        /// <summary>
        /// Cancel the execution of stage.
        /// </summary>
        public void Cancel()
        {
            if (!_cancellationSource.IsCancelled)
            {
                _cancellationSource.Cancel();

                Log.Log(Level.Info, "Received request to close stage {0}", StageName);
            }
        }
    }
}
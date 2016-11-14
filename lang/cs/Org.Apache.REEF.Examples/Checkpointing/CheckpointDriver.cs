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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.IO.Checkpoint.LocalFS;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.Checkpointing
{
    /// <summary>
    /// Example driver for a job with checkpointing service. Allocates one evaluator, executes a checkpointing task there and exits. 
    /// </summary>
    public sealed class CheckpointDriver : IObserver<IAllocatedEvaluator>, IObserver<IDriverStarted>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(CheckpointDriver));

        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private IAllocatedEvaluator _allocatedEvaluator;

        /// <summary>
        /// A driver constructor used by Tang. Tang injects parameters automatically.
        /// </summary>
        /// <param name="evaluatorRequestor"></param>
        [Inject]
        CheckpointDriver(IEvaluatorRequestor evaluatorRequestor)
        {
            _evaluatorRequestor = evaluatorRequestor;
        }

        public void OnNext(IDriverStarted value)
        {
           var request = _evaluatorRequestor.NewBuilder()
                .SetCores(1)
                .SetMegabytes(64)
                .SetNumber(1)
                .Build();
            _evaluatorRequestor.Submit(request);
            Logger.Log(Level.Info, "Evaluator request submitted.");
        }

        public void OnNext(IAllocatedEvaluator evaluator)
        {
            
            _allocatedEvaluator = evaluator;
            Logger.Log(Level.Info, "Allocated evaluator has ID {0} ", _allocatedEvaluator.EvaluatorBatchId);

            var contextConfig = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, _allocatedEvaluator.EvaluatorBatchId + "_context")
                .Build();

            var checkpointServiceConfiguration = LocalFSCheckpointServiceConfiguration.Conf
                .Set(LocalFSCheckpointServiceConfiguration.Path, System.IO.Path.GetTempPath())
                .Build();

            var taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "CheckpointTask")
                .Set(TaskConfiguration.Task, GenericType<CheckpointTask>.Class)
                .Build();

            _allocatedEvaluator.SubmitContextAndServiceAndTask(contextConfig, checkpointServiceConfiguration, taskConfiguration);
            Logger.Log(Level.Info, "Task, context and checkpointing service submitted to evaluator {0} ", _allocatedEvaluator.EvaluatorBatchId);
        }

        // Part of the IObserver<T> interface
        public void OnError(Exception error)
        {
            throw error;
        }

        // Part of the IObserver<T> interface
        public void OnCompleted()
        {
            // ignore
        }
    }
}
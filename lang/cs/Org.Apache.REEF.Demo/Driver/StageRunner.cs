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
using System.Collections.Generic;
using System.Threading;
using Org.Apache.REEF.Demo.Stage;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Demo.Driver
{
    /// <summary>
    /// Manages the execution of a single stage.
    /// </summary>
    internal sealed class StageRunner : IObserver<ICompletedTask>
    {
        private readonly DataSetInfo _dataSetInfo;
        private readonly CountdownEvent _countdownEvent;
        private readonly ISet<IObserver<IStageDriverStarted>> _startHandlers;
        private readonly ISet<IObserver<IStageDriverCompletedTask>> _taskCompletedHandlers;

        [Inject]
        private StageRunner(DataSetInfo dataSetInfo,
                            [Parameter(typeof(StageDriverNamedParameters.StageDriverStartedHandlers))] ISet<IObserver<IStageDriverStarted>> startHandlers,
                            [Parameter(typeof(StageDriverNamedParameters.StageDriverTaskCompletedHandlers))] ISet<IObserver<IStageDriverCompletedTask>> taskCompletedHandlers)
        {
            _dataSetInfo = dataSetInfo;
            _countdownEvent = new CountdownEvent(1);
            _startHandlers = startHandlers;
            _taskCompletedHandlers = taskCompletedHandlers;
        }

        internal void StartStage()
        {
            IStageDriverStarted stageDriverStarted = new StageDriverStarted(_dataSetInfo);
            foreach (var startHandler in _startHandlers)
            {
                startHandler.OnNext(stageDriverStarted);
            }
        }

        internal void EndStage()
        {
            _countdownEvent.Signal();
        }

        internal void AwaitStage()
        {
            _countdownEvent.Wait();
        }

        public void OnNext(ICompletedTask completedTask)
        {
            foreach (var taskCompletedHandler in _taskCompletedHandlers)
            {
                taskCompletedHandler.OnNext(new StageDriverCompletedTask(completedTask));
            }
        }

        public void OnCompleted()
        {
            throw new NotSupportedException();
        }

        public void OnError(Exception e)
        {
            throw new NotSupportedException();
        }
    }
}

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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Collections;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Task
{
    internal sealed class TaskLifeCycle
    {
        private readonly IReadOnlyCollection<IObserver<ITaskStop>> _taskStopHandlers;
        private readonly IReadOnlyCollection<IObserver<ITaskStart>> _taskStartHandlers;
        private readonly IInjector _injector; 

        [Inject]
        private TaskLifeCycle(
            [Parameter(typeof(TaskConfigurationOptions.StartHandlers))] ISet<IObserver<ITaskStart>> taskStartHandlers,
            [Parameter(typeof(TaskConfigurationOptions.StopHandlers))] ISet<IObserver<ITaskStop>> taskStopHandlers,
            IInjector injector)
        {
            _taskStartHandlers = new ReadOnlySet<IObserver<ITaskStart>>(taskStartHandlers);
            _taskStopHandlers = new ReadOnlySet<IObserver<ITaskStop>>(taskStopHandlers);
            _injector = injector;
        }

        public void Start() 
        {
            var taskStart = _injector.GetInstance<TaskStartImpl>();
            foreach (var startHandler in _taskStartHandlers)
            {
                startHandler.OnNext(taskStart);
            }
        }

        public void Stop() 
        {
            var taskStop = _injector.GetInstance<TaskStopImpl>();
            foreach (var stopHandler in _taskStopHandlers)
            {
                stopHandler.OnNext(taskStop);
            }
        }
    }
}

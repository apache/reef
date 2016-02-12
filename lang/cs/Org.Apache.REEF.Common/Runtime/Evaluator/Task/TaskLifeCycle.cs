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
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Task
{
    internal sealed class TaskLifeCycle
    {
        private readonly ISet<IObserver<ITaskStop>> _taskStopHandlers;
        private readonly ISet<IObserver<ITaskStart>> _taskStartHandlers;
        private readonly Optional<IInjector> _injector; 

        [Inject]
        internal TaskLifeCycle() 
            : this(new HashSet<IObserver<ITaskStart>>(), new HashSet<IObserver<ITaskStop>>(), Optional<IInjector>.Empty())
        {
            _taskStartHandlers = new HashSet<IObserver<ITaskStart>>();
            _taskStopHandlers = new HashSet<IObserver<ITaskStop>>();
        }

        [Inject]
        private TaskLifeCycle(
            [Parameter(typeof(TaskConfigurationOptions.StartHandlers))] ISet<IObserver<ITaskStart>> taskStartHandlers,
            [Parameter(typeof(TaskConfigurationOptions.StopHandlers))] ISet<IObserver<ITaskStop>> taskStopHandlers,
            IInjector injector)
            : this(taskStartHandlers, taskStopHandlers, Optional<IInjector>.Of(injector))
        {
        }

        private TaskLifeCycle(
            ISet<IObserver<ITaskStart>> taskStartHandlers,
            ISet<IObserver<ITaskStop>> taskStopHandlers,
            Optional<IInjector> injector)
        {
            _taskStartHandlers = taskStartHandlers;
            _taskStopHandlers = taskStopHandlers;
            _injector = injector;
        }
        
        public void Start() 
        {
            if (!_injector.IsPresent())
            {
                return;
            }

            var taskStart = _injector.Value.GetInstance<TaskStartImpl>();
            foreach (var startHandler in _taskStartHandlers)
            {
                startHandler.OnNext(taskStart);
            }
        }

        public void Stop() 
        {
            if (!_injector.IsPresent())
            {
                return;
            }

            var taskStop = _injector.Value.GetInstance<TaskStopImpl>();
            foreach (var stopHandler in _taskStopHandlers)
            {
                stopHandler.OnNext(taskStop);
            }
        }
    }
}

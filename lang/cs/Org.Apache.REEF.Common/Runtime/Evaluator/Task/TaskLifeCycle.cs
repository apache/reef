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
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Collections;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Task
{
    internal sealed class TaskLifeCycle
    {
        private readonly IReadOnlyCollection<IObserver<ITaskStop>> _taskStopHandlers;
        private readonly IReadOnlyCollection<IObserver<ITaskStart>> _taskStartHandlers;
        private readonly Optional<ITaskStart> _taskStart;
        private readonly Optional<ITaskStop> _taskStop;

        [Inject]
        private TaskLifeCycle(
            [Parameter(typeof(TaskConfigurationOptions.StartHandlers))] ISet<IObserver<ITaskStart>> taskStartHandlers,
            [Parameter(typeof(TaskConfigurationOptions.StopHandlers))] ISet<IObserver<ITaskStop>> taskStopHandlers,
            ITaskStart taskStart,
            ITaskStop taskStop)
            : this(taskStartHandlers, taskStopHandlers, Optional<ITaskStart>.Of(taskStart), Optional<ITaskStop>.Of(taskStop))
        {
        }

        private TaskLifeCycle(
            IEnumerable<IObserver<ITaskStart>> taskStartHandlers,
            IEnumerable<IObserver<ITaskStop>> taskStopHandlers,
            Optional<ITaskStart> taskStart,
            Optional<ITaskStop> taskStop)
        {
            _taskStartHandlers = new ReadOnlySet<IObserver<ITaskStart>>(taskStartHandlers);
            _taskStopHandlers = new ReadOnlySet<IObserver<ITaskStop>>(taskStopHandlers);
            _taskStart = taskStart;
            _taskStop = taskStop;
        }

        public void Start() 
        {
            if (!_taskStart.IsPresent())
            {
                return;
            }

            foreach (var startHandler in _taskStartHandlers)
            {
                startHandler.OnNext(_taskStart.Value);
            }
        }

        public void Stop() 
        {
            if (!_taskStop.IsPresent())
            {
                return;
            }

            foreach (var stopHandler in _taskStopHandlers)
            {
                stopHandler.OnNext(_taskStop.Value);
            }
        }
    }
}

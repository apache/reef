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

using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    internal sealed class TaskInfo
    {
        private readonly TaskStateMachine _taskState;
        private readonly IConfiguration _taskConfiguration;
        private readonly IActiveContext _activeContext;

        /// <summary>
        /// Construct a TaskInfo that wraps task state, task configuration, and active context for submitting the task 
        /// </summary>
        /// <param name="taskState"></param>
        /// <param name="config"></param>
        /// <param name="context"></param>
        internal TaskInfo(TaskStateMachine taskState, IConfiguration config, IActiveContext context)
        {
            _taskState = taskState;
            _taskConfiguration = config;
            _activeContext = context;
        }

        internal TaskStateMachine TaskState
        {
            get { return _taskState; }
        }

        internal IConfiguration TaskConfiguration
        {
            get { return _taskConfiguration; }
        }

        internal IActiveContext ActiveContext
        {
            get { return _activeContext; }
        }
    }
}
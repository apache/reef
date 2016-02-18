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
using Org.Apache.REEF.Common.Tasks.Defaults;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Common.Tasks
{
    public class TaskConfigurationOptions
    {
        [NamedParameter("The Identifier of the Task", "taskid", "Task")]
        public class Identifier : Name<string>
        {
        }

        [NamedParameter(documentation: "The memento to be used for the Task")]
        public class Memento : Name<string>
        {
        }

        [NamedParameter("TaskMessageSource", "messagesource", null)]
        public class TaskMessageSources : Name<ISet<ITaskMessageSource>>
        {
        }

        [NamedParameter(documentation: "The set of event handlers for the TaskStart event.")]
        public class StartHandlers : Name<ISet<IObserver<ITaskStart>>>
        {
        }

        [NamedParameter(documentation: "The set of event handlers for the TaskStop event.")]
        public class StopHandlers : Name<ISet<IObserver<ITaskStop>>>
        {
        }

        [NamedParameter(documentation: "The event handler that receives the close event.")]
        public class CloseHandler : Name<IObserver<ICloseEvent>>
        {
        }

        [NamedParameter(documentation: "The event handler that receives the suspend event.", defaultClass: typeof(DefaultSuspendHandler))]
        public class SuspendHandler : Name<IObserver<ISuspendEvent>>
        {
        }

        [NamedParameter(documentation: "The event handler that receives messages from the driver.")]
        public class MessageHandler : Name<IObserver<IDriverMessage>>
        {
        }
    }
}

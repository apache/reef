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
using Org.Apache.REEF.Bridge.Core.Common.Client.Default;
using Org.Apache.REEF.Common.Client;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Bridge.Core.Common.Client.Config
{
    /// <summary>
    /// Parameters associated with the client.
    /// </summary>
    public sealed class ClientParameters
    {

        [NamedParameter("completed job handler", defaultClass: typeof(DefaultCompletedJobHandler))]
        public sealed class CompletedJobHandler : Name<IObserver<ICompletedJob>>
        {
        }

        [NamedParameter("failed job handler", defaultClass: typeof(DefaultFailedJobHandler))]
        public sealed class FailedJobHandler : Name<IObserver<IFailedJob>>
        {
        }

        [NamedParameter("failed runtime handler", defaultClass: typeof(DefaultFailedRuntimeHandler))]
        public sealed class FailedRuntimeHandler : Name<IObserver<IFailedRuntime>>
        {
        }

        [NamedParameter("job message handler", defaultClass: typeof(DefaultJobMessageHandler))]
        public sealed class JobMessageHandler : Name<IObserver<IJobMessage>>
        {
        }

        [NamedParameter("running job handler", defaultClass: typeof(DefaultRunningJobHandler))]
        public sealed class RunningJobHandler : Name<IObserver<IRunningJob>>
        {
        }

        [NamedParameter("submitted job handler", defaultClass: typeof(DefaultSubmittedJobHandler))]
        public sealed class SubmittedJobHandler : Name<IObserver<ISubmittedJob>>
        {
        }

        [NamedParameter("wake error handler", defaultClass: typeof(DefaultWakeErrorHandler))]
        public sealed class WakeErrorHandler : Name<IObserver<IWakeError>>
        {
        }
    }
}

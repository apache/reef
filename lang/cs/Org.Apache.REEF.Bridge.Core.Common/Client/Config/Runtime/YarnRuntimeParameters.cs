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
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime
{
    internal sealed class YarnRuntimeParameters
    {
        [NamedParameter("FileSystem URL.", defaultValue: "")]
        public sealed class FileSystemUrl : Name<string>
        {
        }

        [NamedParameter("The job priority", defaultValue: "0")]
        public sealed class JobPriority : Name<int>
        {
        }

        [NamedParameter("The job queue.", defaultValue: "")]
        public sealed class JobQueue : Name<string>
        {
        }

        [NamedParameter("Directory used to upload job resources from the client", defaultValue: @"/vol1/tmp")]
        public class JobSubmissionDirectoryPrefix : Name<string>
        {
        }

        /// <summary>
        /// Named parameter that contains a set of serialized tokens.
        /// </summary>
        [NamedParameter("Serialized SurityToken Info", defaultValue: "")]
        public class SecurityTokenStrings : Name<ISet<string>>
        {
        }

        [NamedParameter("Boolean indicating whether we want an unmanged driver.", defaultValue: "false")]
        public sealed class UnmanagedDriver : Name<bool>
        {
        }
    }
}

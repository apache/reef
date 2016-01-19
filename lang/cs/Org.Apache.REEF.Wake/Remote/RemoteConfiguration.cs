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
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Wake.Remote
{
    public sealed class RemoteConfiguration
    {
        [NamedParameter(shortName: "rm_name", documentation: "The name of the remote manager.")]
        public class ManagerName : Name<string>
        {
        }

        [NamedParameter(shortName: "rm_host", documentation: "The host address to be used for messages.")]
        public class HostAddress : Name<string>
        {
        }

        [NamedParameter(shortName: "rm_port", documentation: "The port to be used for messages.")]
        public class Port : Name<int>
        {
        }

        [NamedParameter(documentation: "The codec to be used for messages.")]
        public class MessageCodec : Name<ICodec<Type>>
        {
        }

        [NamedParameter(documentation: "The event handler to be used for exception")]
        public class ErrorHandler : Name<IObserver<Exception>>
        {
        }

        [NamedParameter(shortName: "rm_order", documentation: "Whether or not to use the message ordering guarantee", defaultValue: "true")]
        public class OrderingGuarantee : Name<bool>
        {
        }
    }
}

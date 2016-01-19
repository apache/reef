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

namespace Org.Apache.REEF.Driver.Bridge
{
    /// <summary>
    /// A class that holds all .NET handles for Java InterOp calls.
    /// </summary>
    public sealed class BridgeHandlerManager
    {
        public ulong AllocatedEvaluatorHandler { get; internal set; }

        public ulong TaskMessageHandler { get; internal set; }

        public ulong ActiveContextHandler { get; internal set; }

        public ulong FailedTaskHandler { get; internal set; }

        public ulong RunningTaskHandler { get; internal set; }

        public ulong CompletedTaskHandler { get; internal set; }

        public ulong SuspendedTaskHandler { get; internal set; }

        public ulong FailedEvaluatorHandler { get; internal set; }

        public ulong CompletedEvaluatorHandler { get; internal set; }

        public ulong ClosedContextHandler { get; internal set; }

        public ulong FailedContextHandler { get; internal set; }

        public ulong ContextMessageHandler { get; internal set; }

        public ulong DriverRestartActiveContextHandler { get; internal set; }

        public ulong DriverRestartRunningTaskHandler { get; internal set; }

        public ulong DriverRestartCompletedHandler { get; internal set; }

        public ulong DriverRestartFailedEvaluatorHandler { get; internal set; }

        public ulong HttpServerHandler { get; internal set; }

        public ulong ProgressProvider { get; internal set; }
    }
}

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

namespace Org.Apache.REEF.Network.Elastic.Config
{
    ///<summary>
    ///Class wrapping the configuration option for the elastic 
    ///group communication service.
    ///</summary>
    public sealed class ElasticServiceConfigurationOptions
    {
        [NamedParameter("Number of Evaluators")]
        public sealed class NumEvaluators : Name<int>
        {
        }

        [NamedParameter("Number of Servers")]
        public sealed class NumServers : Name<int>
        {
        }

        [NamedParameter("Number of Workers")]
        public sealed class NumWorkers : Name<int>
        {
        }

        [NamedParameter(Documentation = "Number of retry when a failure occurs", DefaultValue = "1")]
        public sealed class RetryAfterFailure : Name<int>
        {
        }

        [NamedParameter(Documentation = "Starting port for TcpPortProvider", DefaultValue = "8900")]
        public sealed class StartingPort : Name<int>
        {
        }

        [NamedParameter(Documentation = "Port Range count for TcpPortProvider", DefaultValue = "1000")]
        public sealed class PortRange : Name<int>
        {
        }

        [NamedParameter("Driver identifier")]
        public sealed class DriverId : Name<string>
        {
        }

        [NamedParameter("Default Group name", defaultValue: "Subscription1")]
        public sealed class DefaultSubscriptionName : Name<string>
        {
        }

        [NamedParameter("Number of tasks", defaultValue: "5")]
        public sealed class NumberOfTasks : Name<int>
        {
        }

        [NamedParameter("Serialized subscriptions configuration")]
        public sealed class SerializedSubscriptionConfigs : Name<ISet<string>>
        {
        }

        [NamedParameter("Timeout after which computation is consider inactive", defaultValue: "600000")]
        public sealed class Timeout : Name<int>
        {
        }

        [NamedParameter("Number of retry to send a message", defaultValue: "50")]
        public sealed class SendRetry : Name<int>
        {
        }

        [NamedParameter("Number of millisecond between each message retry", defaultValue: "1000")]
        public sealed class RetryWaitTime : Name<int>
        {
        }

        [NamedParameter("Number of failures before a task abort the task set", defaultValue: "100")]
        public sealed class NumTaskFailures : Name<int>
        {
        }

        [NamedParameter(Documentation = "Rack name used when a new evaluator is requested after a failure", DefaultValue = "WonderlandRack")]
        public sealed class NewEvaluatorRackName : Name<string>
        {
        }

        [NamedParameter(Documentation = "Batch id used when a new evaluator is requested after a failure", DefaultValue = "IterateBroadcast")]
        public sealed class NewEvaluatorBatchId : Name<string>
        {
        }

        [NamedParameter(Documentation = "Number of cores used when a new evaluator is requested after a failure", DefaultValue = "1")]
        public sealed class NewEvaluatorNumCores : Name<int>
        {
        }

        [NamedParameter(Documentation = "Memory size used when a new evaluator is requested after a failure", DefaultValue = "512")]
        public sealed class NewEvaluatorMemorySize : Name<int>
        {
        }

        [NamedParameter("Number of checkpoints to store per operator", defaultValue: "1")]
        public sealed class NumCheckpoints : Name<int>
        {
        }
    }
}

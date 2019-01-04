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

using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Examples.Elastic;

namespace Org.Apache.REEF.Network.Examples.Client
{
    public sealed class ElasticBroadcastClientWithMultipleFailures : ElasticBroadcastClient
    {
        public ElasticBroadcastClientWithMultipleFailures(bool runOnYarn, int numTasks, int startingPortNo, int portRange)
            : base (runOnYarn, numTasks, startingPortNo, portRange)
        {
        }

        protected override string JobIdentifier
        {
            get { return "ElasticBroadcastWithFailure"; }
        }

        protected override IConfiguration GetDriverConf()
        {
            return DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnDriverStarted, GenericType<ElasticBroadcastDriverWithMultipleFailures>.Class)
                    .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ElasticBroadcastDriverWithMultipleFailures>.Class)
                    .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<ElasticBroadcastDriverWithMultipleFailures>.Class)
                    .Set(DriverConfiguration.OnContextActive, GenericType<ElasticBroadcastDriverWithMultipleFailures>.Class)
                    .Set(DriverConfiguration.OnTaskRunning, GenericType<ElasticBroadcastDriverWithMultipleFailures>.Class)
                    .Set(DriverConfiguration.OnTaskCompleted, GenericType<ElasticBroadcastDriverWithMultipleFailures>.Class)
                    .Set(DriverConfiguration.OnTaskFailed, GenericType<ElasticBroadcastDriverWithMultipleFailures>.Class)
                    .Set(DriverConfiguration.OnTaskMessage, GenericType<ElasticBroadcastDriverWithMultipleFailures>.Class)
                    .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
                    .Build();
        }
    }
}

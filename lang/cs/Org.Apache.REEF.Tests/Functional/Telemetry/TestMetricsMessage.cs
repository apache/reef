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

using Org.Apache.REEF.Common.Telemetry;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Telemetry
{
    [Collection("FunctionalTests")]
    public class TestMetricsMessage : ReefFunctionalTest
    {
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test Evaluator Metrics send from evaluator to Metrics Service.")]
        public void TestMetricsMessages()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(), typeof(MetricsDriver), 1, "sendMessages", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);
            string[] lines = ReadLogFile(DriverStdout, "driver", testFolder, 240);
            var receivedCounterMessage = GetMessageCount(lines, "Received 2 counters with context message:");
            Assert.True(receivedCounterMessage > 1);

            var messageCount = GetMessageCount(lines, MetricsDriver.EventPrefix);
            Assert.Equal(4, messageCount);

            CleanUp(testFolder);
        }

        private static IConfiguration DriverConfigurations()
        {
            var driverBasicConfig = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<MetricsDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<MetricsDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<MetricsDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<MetricsDriver>.Class)
                .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
                .Build();

            var metricServiceConfig = MetricsServiceConfigurationModule.ConfigurationModule
                .Set(MetricsServiceConfigurationModule.OnMetricsSink, GenericType<DefaultMetricsSink>.Class)
                .Set(MetricsServiceConfigurationModule.CounterSinkThreshold, "5")
                .Build();

            var driverMetricConfig = DriverMetricsObserverConfigurationModule.ConfigurationModule.Build();

            return Configurations.Merge(driverBasicConfig, metricServiceConfig, driverMetricConfig);
        }
    }
}

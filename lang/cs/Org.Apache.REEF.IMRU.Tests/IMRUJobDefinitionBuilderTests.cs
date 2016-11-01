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

using System.Reflection;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.IMRU.Tests
{
    public class IMRUJobDefinitionBuilderTests
    {
        [Fact]
        public void JobDefinitionBuilderCancellationConfigIsOptional()
        {
            var builder = CreateTestBuilder();
            var definition = builder.Build();

            var defaultConfig = typeof(IMRUJobDefinitionBuilder)
                .GetField("EmptyConfiguration", BindingFlags.NonPublic | BindingFlags.Static)
                .GetValue(builder) as IConfiguration;

            Assert.Same(defaultConfig, definition.JobCancelSignalConfiguration);
        }

        [Fact]
        public void JobDefinitionBuilderCancellationConfigIsSetToNull()
        {
            var definition = CreateTestBuilder()
               .SetJobCancellationConfiguration(null)
               .Build();

            Assert.Null(definition.JobCancelSignalConfiguration);
        }

        [Fact]
        public void JobDefinitionBuilderSetsJobCancellationConfig()
        {
            var cancelSignalConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IJobCancelledDetector>.Class,
                    GenericType<JobCancellationDetectorAlwaysFalse>.Class)
                .Build();

            var definition = CreateTestBuilder()
                .SetJobCancellationConfiguration(cancelSignalConfig)
                .Build();

            Assert.NotNull(definition.JobCancelSignalConfiguration);
            Assert.Same(cancelSignalConfig, definition.JobCancelSignalConfiguration);
        }

        private IMRUJobDefinitionBuilder CreateTestBuilder()
        {
            var testConfig = TangFactory.GetTang().NewConfigurationBuilder().Build();

            return new IMRUJobDefinitionBuilder()
                .SetJobName("Test")
                .SetMapFunctionConfiguration(testConfig)
                .SetMapInputCodecConfiguration(testConfig)
                .SetUpdateFunctionCodecsConfiguration(testConfig)
                .SetReduceFunctionConfiguration(testConfig)
                .SetUpdateFunctionConfiguration(testConfig);
        }
    }
}

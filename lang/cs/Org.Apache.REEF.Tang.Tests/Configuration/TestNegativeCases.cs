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
using System.Linq;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.Configuration
{
    public class TestNegativeCases
    {
        [Fact]
        public void TestDuplicatedNamedParameterbinding()
        {
            string msg = null;
            try
            {
                var c = TangFactory.GetTang().NewConfigurationBuilder()
                    .BindStringNamedParam<NamedString>("abc")
                    .BindStringNamedParam<NamedString>("def")
                    .Build();
            }
            catch (ArgumentException e)
            {
                msg = e.Message;
            }
            Assert.NotNull(msg);
            Assert.True(msg.Contains(ConfigurationBuilderImpl.DuplicatedEntryForNamedParamater));
        }

        [Fact]
        public void TestAddOptionalConfiguration_NullIsAllowed()
        {
            var builder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<NamedString>("abc");

            builder.AddOptionalConfiguration(null);
        }

        [Fact]
        public void TestAddOptionalConfiguration_ValidConfig()
        {
            var configurationBuilder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<NamedString>("abc");

            var expectedOptionalValue = "def";
            var additionalConfiguration = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<NamedString2>(expectedOptionalValue)
                .Build();

            var configuration = configurationBuilder
                .AddOptionalConfiguration(additionalConfiguration)
                .Build();

            Assert.NotNull(configuration);
            var parameters = configuration.GetNamedParameters().ToList();
            var optionalParameter = parameters.Single(p => p.GetShortName() == "NamedString2");
            Assert.NotNull(optionalParameter);
            Assert.Same(expectedOptionalValue, configuration.GetNamedParameter(optionalParameter));
        }

        [NamedParameter(Documentation = "test", ShortName = "NamedString")]
        class NamedString : Name<string>
        {
        }

        [NamedParameter(Documentation = "test2", ShortName = "NamedString2")]
        class NamedString2 : Name<string>
        {
        }
    }
}
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

using Org.Apache.REEF.Client.Multi;
using Org.Apache.REEF.Common.Runtime;

using Xunit;

namespace Org.Apache.REEF.Client.Tests
{
    public class MultiRuntimeConfigurationBuilderTests
    {
        [Fact]
        public void ThrowsOnNotSupportedRuntimes()
        {
            MultiRuntimeConfigurationBuilder mrcb = new MultiRuntimeConfigurationBuilder();
            Assert.Throws<ArgumentException>(() =>
            {
                mrcb.AddRuntime(RuntimeName.Mesos);
            });
        }

        [Fact]
        public void ThrowsWhenMoreThanOneRuntimeAndNoDefault()
        {
            MultiRuntimeConfigurationBuilder mrcb = new MultiRuntimeConfigurationBuilder();
            mrcb.AddRuntime(RuntimeName.Local);
            mrcb.AddRuntime(RuntimeName.Yarn);
            mrcb.SetSubmissionRuntime(SubmissionRuntimeName.YarnRest);
            mrcb.SetMaxEvaluatorsNumberForLocalRuntime(1);
            Assert.Throws<ArgumentException>(() =>
            {
                mrcb.Build();
            });
        }

        [Fact]
        public void NotThrowsWhenOneRuntimeAndNoDefault()
        {
            MultiRuntimeConfigurationBuilder mrcb = new MultiRuntimeConfigurationBuilder();
            mrcb.AddRuntime(RuntimeName.Local);
            mrcb.SetSubmissionRuntime(SubmissionRuntimeName.YarnRest);
            mrcb.SetMaxEvaluatorsNumberForLocalRuntime(1);
            var config = mrcb.Build();
            Assert.NotNull(config);
        }

        [Fact]
        public void ThrowsWhenNoSubmissionRuntimeIsSet()
        {
            MultiRuntimeConfigurationBuilder mrcb = new MultiRuntimeConfigurationBuilder();
            mrcb.AddRuntime(RuntimeName.Local);
            mrcb.SetMaxEvaluatorsNumberForLocalRuntime(1);
            Assert.Throws<ArgumentException>(() =>
            {
                mrcb.Build();
            });
        }

        [Fact]
        public void ThrowsWhenMaxEvaluatorsNotSetForLocalRuntime()
        {
            MultiRuntimeConfigurationBuilder mrcb = new MultiRuntimeConfigurationBuilder();
            mrcb.AddRuntime(RuntimeName.Local);
            mrcb.SetSubmissionRuntime(SubmissionRuntimeName.YarnRest);
            Assert.Throws<ArgumentException>(() =>
            {
                mrcb.Build();
            });
        }

        [Fact]
        public void ThrowsWhenHDISubmissionRuntimeAndBlobConnectionStringNotSet()
        {
                MultiRuntimeConfigurationBuilder mrcb = new MultiRuntimeConfigurationBuilder();
            mrcb =
                mrcb.AddRuntime(RuntimeName.Local)
                    .SetMaxEvaluatorsNumberForLocalRuntime(1)   
                    .SetSubmissionRuntime(SubmissionRuntimeName.HDInsight)
                    .SetAzureBlobContainerName("container")
                    .SetHDInsightPassword("pwd")
                    .SetHDInsightUrl("http://url/")
                    .SetHDInsightUsername("username");
            Assert.Throws<ArgumentException>(() =>
            {
                    mrcb.Build();
            });
        }

        [Fact]
        public void ThrowsWhenHDISubmissionRuntimeAndBlobContainerNotSet()
        {
                MultiRuntimeConfigurationBuilder mrcb = new MultiRuntimeConfigurationBuilder();
            mrcb =
                mrcb.AddRuntime(RuntimeName.Local)
                    .SetMaxEvaluatorsNumberForLocalRuntime(1)
                    .SetSubmissionRuntime(SubmissionRuntimeName.HDInsight)
                    .SetAzureBlobConnectionString("connection string")
                    .SetHDInsightPassword("pwd")
                    .SetHDInsightUrl("http://url/")
                    .SetHDInsightUsername("username");
            Assert.Throws<ArgumentException>(() =>
            {
                    mrcb.Build();
            });
        }

        [Fact]
        public void ThrowsWhenHDISubmissionRuntimeAndPasswordNotSet()
        {
                MultiRuntimeConfigurationBuilder mrcb = new MultiRuntimeConfigurationBuilder();
            mrcb =
                mrcb.AddRuntime(RuntimeName.Local)
                    .SetMaxEvaluatorsNumberForLocalRuntime(1)
                    .SetSubmissionRuntime(SubmissionRuntimeName.HDInsight)
                    .SetAzureBlobConnectionString("connection string")
                    .SetAzureBlobContainerName("container")
                    .SetHDInsightUrl("http://url/")
                    .SetHDInsightUsername("username");
            Assert.Throws<ArgumentException>(() =>
            {
                    mrcb.Build();
            });
        }

        [Fact]
        public void ThrowsWhenHDISubmissionRuntimeAndUrlNotSet()
        {
                MultiRuntimeConfigurationBuilder mrcb = new MultiRuntimeConfigurationBuilder();
            mrcb =
                mrcb.AddRuntime(RuntimeName.Local)
                    .SetMaxEvaluatorsNumberForLocalRuntime(1)
                    .SetSubmissionRuntime(SubmissionRuntimeName.HDInsight)
                    .SetAzureBlobConnectionString("connection string")
                    .SetAzureBlobContainerName("container")
                    .SetHDInsightPassword("pwd")
                    .SetHDInsightUsername("username");
            Assert.Throws<ArgumentException>(() =>
            {
                    mrcb.Build();
            });
        }

        [Fact]
        public void ThrowsWhenHDISubmissionRuntimeAndUserNameNotSet()
        {
                MultiRuntimeConfigurationBuilder mrcb = new MultiRuntimeConfigurationBuilder();
            mrcb.AddRuntime(RuntimeName.Local)
                .SetMaxEvaluatorsNumberForLocalRuntime(1)
                .SetSubmissionRuntime(SubmissionRuntimeName.HDInsight)
                .SetAzureBlobConnectionString("connection string")
                .SetAzureBlobContainerName("container")
                .SetHDInsightPassword("pwd")
                .SetHDInsightUrl("http://url/");
            Assert.Throws<ArgumentException>(() =>
            {
                    mrcb.Build();
            });
        }
    }
}

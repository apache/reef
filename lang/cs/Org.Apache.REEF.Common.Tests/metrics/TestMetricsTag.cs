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

using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Common.Metrics.MutableMetricsLayer;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.Common.Tests.Metrics
{
    /// <summary>
    /// Contains functions to test <see cref="MetricsTag"/> clkass.
    /// </summary>
    public sealed class TestMetricsTag
    {
        /// <summary>
        /// Tests <see cref="MetricsInfoImpl"/>.
        /// </summary>
        [Fact]
        public void TestMetricsInfoImpl()
        {
            const string name = "testname";
            const string desc = "testdesc";

            MetricsInfoImpl impl = new MetricsInfoImpl(name, desc);
            Assert.Equal(name, impl.Name);
            Assert.Equal(desc, impl.Description);
        }

        /// <summary>
        /// Tests different functions of <see cref="MetricsTag"/>
        /// </summary>
        [Fact]
        public void TestMetricsTagClass()
        {
            const string name = "tagtest";
            const string otherName = "othertagtest";
            const string desc = "tagtestdesc";
            const string otherDesc = "othertagtestdesc";
            const string tagValue = "tagvalue";
            const string otherTagValue = "othertagvalue";
            IMetricsFactory factory = TangFactory.GetTang().NewInjector().GetInstance<IMetricsFactory>();

            IMetricsInfo info = new MetricsInfoImpl(name, desc);
            MetricsTag tag = factory.CreateTag(info, tagValue);

            Assert.Equal(name, tag.Name);
            Assert.Equal(desc, tag.Description);
            Assert.Equal(tagValue, tag.Value);

            MetricsTag sameTag = factory.CreateTag(info, tagValue);
            Assert.True(tag.Equals(sameTag));

            MetricsTag otherTag = factory.CreateTag(info, otherTagValue);
            Assert.False(tag.Equals(otherTag));

            otherTag = factory.CreateTag(new MetricsInfoImpl(otherName, desc), tagValue);
            Assert.False(tag.Equals(otherTag));

            otherTag = factory.CreateTag(new MetricsInfoImpl(name, otherDesc), otherTagValue);
            Assert.False(tag.Equals(otherTag));

            string expectedToString = "Tag Information: " + "Name: " + info.Name + ", Description: " + info.Description +
                                      ", Tag Value: " + tagValue;
            Assert.Equal(expectedToString, tag.ToString());
        }
    }
}

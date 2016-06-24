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

using Org.Apache.REEF.IMRU.Examples.NaturalSum;
using Org.Apache.REEF.IMRU.InProcess;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.IMRU.Tests
{
    /// <summary>
    /// Tests of the natural sum job.
    /// </summary>
    public class NaturalSumTest
    {
        private const int NumberOfMappers = 7;

        /// <summary>
        /// Tests the natural sum job using the in-process IMRU implementation.
        /// </summary>
        [Fact]
        public void TestNaturalSumInProcess()
        {
            var naturalSumJob =
                TangFactory.GetTang()
                    .NewInjector(
                        InProcessIMRUConfiguration.ConfigurationModule
                            .Set(InProcessIMRUConfiguration.NumberOfMappers, NumberOfMappers.ToString())
                            .Build())
                    .GetInstance<NaturalSum>();
            var result = naturalSumJob.Run(NumberOfMappers, string.Empty, TangFactory.GetTang().NewConfigurationBuilder().Build());
            var expected = NumberOfMappers * (NumberOfMappers + 1) / 2; // expected = 1 + 2 + ... + NumberOfMappers
            Assert.True(expected == result, string.Format("The result of the run should be {0} but was {1}.", expected, result));
        }
    }
}

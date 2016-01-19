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

using Org.Apache.REEF.IMRU.Examples.MapperCount;
using Org.Apache.REEF.IMRU.InProcess;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.IMRU.Tests
{
    /// <summary>
    /// Tests of the mapper counting job.
    /// </summary>
    public class MapperCountTest
    {
        private const int NumberOfMappers = 7;

        /// <summary>
        /// Tests the mapper counting job using the in-process IMRU implementation.
        /// </summary>
        [Fact]
        public void TestMapperCountInProcess()
        {
            var tested =
                TangFactory.GetTang()
                    .NewInjector(
                        InProcessIMRUConfiguration.ConfigurationModule
                            .Set(InProcessIMRUConfiguration.NumberOfMappers, NumberOfMappers.ToString())
                            .Build())
                    .GetInstance<MapperCount>();
            var result = tested.Run(NumberOfMappers, string.Empty, TangFactory.GetTang().NewConfigurationBuilder().Build());
            Assert.True(NumberOfMappers == result, "The result of the run should be the number of Mappers.");
        }
    }
}
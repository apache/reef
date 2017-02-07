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
using Org.Apache.REEF.Common.Telemetry;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.Common.Tests.Telemetry
{
    public class CounterTests
    {
        [Fact]
        public void TestEvaluatorMetrics()
        {
            var metrics = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var counters = metrics.GetMetricsCounters();

            counters.TryRegisterCounter("counter1");
            counters.TryRegisterCounter("counter2");
            counters.Increament("counter1", 3);
            counters.Increament("counter1", 1);
            counters.Increament("counter2", 2);
            counters.Increament("counter2", 3);

            var counterStr = metrics.Serialize();
            var d = counters.Deserialize(counterStr);
            int c1;
            d.TryGetValue("counter1", out c1);
            Assert.Equal(4, c1);
            int c2;
            d.TryGetValue("counter2", out c2);
            Assert.Equal(5, c2);
        }

        [Fact]
        public void TestDuplicatedCounters()
        {
            var counters = CreateCounters();
            counters.TryRegisterCounter("counter1");

            Assert.False(counters.TryRegisterCounter("counter1"));
        }

        [Fact]
        public void TestNoExistCounter()
        {
            var counters = CreateCounters();
            Action increament = () => counters.Increament("counter1", 2);
            Assert.Throws<ApplicationException>(increament);
        }

        private static ICounters CreateCounters()
        {
            var m = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var c = m.GetMetricsCounters();
            return c;
        }
    }
}

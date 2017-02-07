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
using System.Collections.Generic;
using Org.Apache.REEF.Common.Telemetry;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.Common.Tests.Telemetry
{
    public class CounterTests
    {
        /// <summary>
        /// Test ICounters and IEvaluatorMetrics API.
        /// </summary>
        [Fact]
        public void TestEvaluatorMetrics()
        {
            var metrics = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var counters = metrics.GetMetricsCounters();
            counters.TryRegisterCounter("counter1", "counter1 description");
            counters.TryRegisterCounter("counter2", "counter2 description");
            ValidateCounter(counters, "counter1", 0);
            ValidateCounter(counters, "counter2", 0);

            counters.Increment("counter1", 3);
            counters.Increment("counter1", 1);
            counters.Increment("counter2", 2);
            counters.Increment("counter2", 3);
            ValidateCounter(counters, "counter1", 4);
            ValidateCounter(counters, "counter2", 5);

            var counterStr = metrics.Serialize();

            var metrics2 = new EvaluatorMetrics(counterStr);
            var counters2 = metrics2.GetMetricsCounters();
            ValidateCounter(counters2, "counter1", 4);
            ValidateCounter(counters2, "counter2", 5);
        }

        /// <summary>
        /// Test TryRegisterCounter with a duplicated counter name
        /// </summary>
        [Fact]
        public void TestDuplicatedCounters()
        {
            var counters = CreateCounters();
            counters.TryRegisterCounter("counter1", "counter1 description");
            Assert.False(counters.TryRegisterCounter("counter1", "counter1 description"));
        }

        /// <summary>
        /// Test Increment for a non-registered counter.
        /// </summary>
        [Fact]
        public void TestNoExistCounter()
        {
            var counters = CreateCounters();
            Action increment = () => counters.Increment("counter1", 2);
            Assert.Throws<ApplicationException>(increment);
        }

        private static void ValidateCounter(ICounters counters, string name, int expectedValue)
        {
            ICounter c1;
            counters.TryGetValue(name, out c1);
            Assert.Equal(expectedValue, c1.Value);
        }

        private static ICounters CreateCounters()
        {
            var m = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var c = m.GetMetricsCounters();
            return c;
        }
    }
}

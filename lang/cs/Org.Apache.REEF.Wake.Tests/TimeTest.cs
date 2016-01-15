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
using Org.Apache.REEF.Wake.Time.Event;
using Xunit;

namespace Org.Apache.REEF.Wake.Tests
{
    public sealed class TimeTest
    {
        private const int RandomSeed = 5;

        [Fact]
        public void BadTime()
        {
            Assert.Throws<ArgumentException>(() => new StartTime(-1));
        }

        [Fact]
        public void SimpleTimeComparison()
        {
            long ts = new Random(RandomSeed).Next(0, 100);
            var st1 = new StartTime(ts);
            var st2 = new StartTime(ts);
            Assert.False(ReferenceEquals(st1, st2));
            Assert.Equal(st1, st2);
            Assert.Equal(st1.GetHashCode(), st2.GetHashCode());
            Assert.True(st1.CompareTo(st2) == 0);
        }

        [Fact]
        public void TestTimeSort()
        {
            const int testLen = 500;
            var testArr = new Time.Time[testLen];
            var r = new Random(RandomSeed);

            for (var i = 0; i < testLen; i++)
            {
                var time = new StartTime(r.Next(0, 10000));
                testArr[i] = time;
            }

            Array.Sort(testArr);

            for (var i = 0; i < testLen - 1; i++)
            {
                Assert.True(testArr[i].TimeStamp <= testArr[i + 1].TimeStamp);
                Assert.True(testArr[i].CompareTo(testArr[i + 1]) <= 0);
                Assert.True(testArr[i + 1].CompareTo(testArr[i]) >= 0);
            }
        }
    }
}

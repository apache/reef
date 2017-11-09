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

using Org.Apache.REEF.Client.Local.TestRunner.FileWritingAssert;
using Xunit;

namespace Org.Apache.REEF.Client.Tests
{
    /// <summary>
    /// Tests for the File Writing Assert
    /// </summary>
    public class TestFileWritingAssert
    {
        [Fact]
        public void TestTestResult()
        {
            TestResult x = new TestResult();
            Assert.Equal(0, x.NumberOfPassedAsserts);
            Assert.Equal(0, x.NumberOfFailedAsserts);

            x.Add(true, "Something went right");
            Assert.Equal(1, x.NumberOfPassedAsserts);
            Assert.Equal(0, x.NumberOfFailedAsserts);
            Assert.True(x.AllTestsSucceeded);

            x.IsTrue("Something else went right");
            Assert.Equal(2, x.NumberOfPassedAsserts);
            Assert.Equal(0, x.NumberOfFailedAsserts);
            Assert.True(x.AllTestsSucceeded);

            x.Add(false, "Something went wrong");
            Assert.Equal(2, x.NumberOfPassedAsserts);
            Assert.Equal(1, x.NumberOfFailedAsserts);
            Assert.False(x.AllTestsSucceeded);

            x.IsFalse("Something else went wrong");
            Assert.Equal(2, x.NumberOfPassedAsserts);
            Assert.Equal(2, x.NumberOfFailedAsserts);
            Assert.False(x.AllTestsSucceeded);
        }
        
        [Fact]
        public void TestTestResultFail()
        {
            var x = TestResult.Fail("OMG! It failed!");
            Assert.Equal(0, x.NumberOfPassedAsserts);
            Assert.Equal(1, x.NumberOfFailedAsserts);
            Assert.False(x.AllTestsSucceeded);
        }

        [Fact]
        public void TestTestResultSerialization()
        {
            TestResult before = new TestResult();
            before.Add(true, "Something went right");
            before.Add(true, "Something else went right");
            before.Add(false, "Something went wrong");

            TestResult after = TestResult.FromJson(before.ToJson());

            Assert.NotNull(after);
            Assert.Equal(1, after.NumberOfFailedAsserts);
            Assert.Equal(2, after.NumberOfPassedAsserts);

            Assert.Equal(before.ToJson(), after.ToJson());
        }
    }
}
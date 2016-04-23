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
using Org.Apache.REEF.Utilities;
using Xunit;

namespace Org.Apache.REEF.Tests.Utility
{
    public sealed class TestOptional
    {
        [Fact]
        public void TestOptionalNullableThrowsWhenNull()
        {
            Assert.Throws<ArgumentNullException>(() => Optional<TestClass>.Of(null));
        }

        [Fact]
        public void TestOptionalNullableOfNullableNotPresent()
        {
            var optional = Optional<TestClass>.OfNullable(null);
            Assert.False(optional.IsPresent());
        }

        [Fact]
        public void TestOptionalNullableValuePresent()
        {
            const int expected = 1;
            var optional = Optional<TestClass>.Of(new TestClass(expected));
            Assert.True(optional.IsPresent());
            Assert.Equal(expected, optional.Value.Integer);
        }

        [Fact]
        public void TestOptionalNullableEquality()
        {
            const int intVal = 1;
            var optional1 = Optional<TestClass>.Of(new TestClass(intVal));
            var optional2 = Optional<TestClass>.Of(new TestClass(intVal));
            Assert.Equal(optional1, optional2);
            Assert.Equal(optional1.GetHashCode(), optional2.GetHashCode());
        }

        [Fact]
        public void TestOptionalNullableEmpty()
        {
            var empty1 = Optional<TestClass>.Empty();
            var empty2 = Optional<TestClass>.Empty();
            Assert.Equal(null, empty1.Value);
            Assert.False(empty1.IsPresent());
            Assert.Equal(empty1, empty2);
            Assert.Equal(empty1.GetHashCode(), empty2.GetHashCode());
        }

        [Fact]
        public void TestOptionalNullableOrElse()
        {
            const int expectedInt = 1;
            var srcObj = new TestClass(expectedInt);
            var elseObj = Optional<TestClass>.Empty().OrElse(srcObj);
            Assert.Equal(srcObj, elseObj);
            Assert.Equal(expectedInt, elseObj.Integer);
            Assert.True(ReferenceEquals(srcObj, elseObj));
            Assert.Null(Optional<TestClass>.Empty().OrElse(null));

            const int unexpectedInt = 2;
            var unexpectedObj = new TestClass(unexpectedInt);
            var ifObj = Optional<TestClass>.Of(srcObj).OrElse(unexpectedObj);
            Assert.NotNull(ifObj);
            Assert.NotEqual(unexpectedObj, ifObj);
            Assert.NotEqual(unexpectedInt, ifObj.Integer);
            Assert.Equal(expectedInt, ifObj.Integer);
            Assert.Equal(srcObj, ifObj);
            Assert.True(ReferenceEquals(srcObj, ifObj));
        }

        [Fact]
        public void TestOptionalNotNullableIsPresent()
        {
            Assert.True(Optional<int>.Of(default(int)).IsPresent());
            Assert.True(Optional<int>.Of(1).IsPresent());
        }

        [Fact]
        public void TestOptionalNotNullableEmpty()
        {
            var empty = Optional<int>.Empty();
            Assert.False(empty.IsPresent());
            Assert.Equal(default(int), empty.Value);
        }

        [Fact]
        public void TestOptionalNotNullableEquality()
        {
            var optional1 = Optional<int>.Of(1);
            var optional1Equiv = Optional<int>.Of(1);
            var optional1OfNullableEquiv = Optional<int>.OfNullable(1);
            var optional2 = Optional<int>.Of(2);

            Assert.Equal(optional1, optional1Equiv);
            Assert.Equal(optional1.GetHashCode(), optional1Equiv.GetHashCode());
            Assert.Equal(optional1, optional1OfNullableEquiv);
            Assert.Equal(optional1.GetHashCode(), optional1OfNullableEquiv.GetHashCode());
            Assert.NotEqual(optional1, optional2);

            var empty1 = Optional<int>.Empty();
            var empty2 = Optional<int>.Empty();
            Assert.Equal(empty1, empty2);
            Assert.Equal(empty1.Value, empty2.Value);
            Assert.Equal(empty1.GetHashCode(), empty2.GetHashCode());
        }

        [Fact]
        public void TestOptionalNotNullableOrElse()
        {
            const int defaultInt = default(int);
            const int expectedInt = 1;
            const int notExpectedInt = 2;
            Assert.Equal(defaultInt, Optional<int>.Of(defaultInt).OrElse(notExpectedInt));
            Assert.Equal(expectedInt, Optional<int>.Of(expectedInt).OrElse(notExpectedInt));
            Assert.Equal(expectedInt, Optional<int>.Empty().OrElse(expectedInt));
        }

        private sealed class TestClass
        {
            private readonly int _i;

            public TestClass(int i)
            {
                _i = i;
            }

            public int Integer
            {
                get { return _i; }
            }

            private bool Equals(TestClass other)
            {
                return _i == other._i;
            }

            public override bool Equals(object obj)
            {
                var that = obj as TestClass;
                return that != null && Equals(that);
            }

            public override int GetHashCode()
            {
                return _i;
            }
        }
    }
}
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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.Injection
{
    public class TestNamedParameter
    {
        [Fact]
        public void TestOptionalParameter()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<StringTest.NamedString, string>(GenericType<StringTest.NamedString>.Class, "foo");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<StringTest>();
            o.Verify("foo");
        }

        [Fact]
        public void TestOptionalParameterWithDefault()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<StringTest>();
            o.Verify(" ");
        }

        [Fact]
        public void TestBoolParameter()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<BooleanTest.NamedBool, bool>(GenericType<BooleanTest.NamedBool>.Class, "true");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<BooleanTest>();
            o.Verify(true);
        }

        [Fact]
        public void TestBoolUpperCaseParameter()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<BooleanTest.NamedBool, bool>(GenericType<BooleanTest.NamedBool>.Class, "True");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<BooleanTest>();
            o.Verify(true);
        }

        [Fact]
        public void TestBoolParameterWithDefault()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<BooleanTest>();
            o.Verify(false);
        }

        [Fact]
        public void TestByteParameter()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<ByteTest.NamedByte, byte>(GenericType<ByteTest.NamedByte>.Class, "6");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<ByteTest>();
            o.Verify(6);
        }

        [Fact]
        public void TestByteArrayParameter()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            string input = "abcde";
            cb.BindNamedParameter<ByteArrayTest.NamedByteArray, byte[]>(GenericType<ByteArrayTest.NamedByteArray>.Class, input);
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<ByteArrayTest>();

            byte[] bytes = new byte[input.Length * sizeof(char)];
            System.Buffer.BlockCopy(input.ToCharArray(), 0, bytes, 0, bytes.Length);
            Assert.True(o.Verify(bytes));
        }

        [Fact]
        public void TestCharParameter()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<CharTest.NamedChar, char>(GenericType<CharTest.NamedChar>.Class, "C");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<CharTest>();
            o.Verify('C');
        }

        [Fact]
        public void TestShortParameter()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<Int16Test.NamedShort, short>(GenericType<Int16Test.NamedShort>.Class, "8");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<Int16Test>();
            o.Verify(8);
        }

        [Fact]
        public void TestIntParameter()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<Int32Test.NamedInt, int>(GenericType<Int32Test.NamedInt>.Class, "8");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<Int32Test>();
            o.Verify(8);
        }

        [Fact]
        public void TestLongParameter()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<Int64Test.NamedLong, long>(GenericType<Int64Test.NamedLong>.Class, "8777");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<Int64Test>();
            o.Verify(8777);
        }

        [Fact]
        public void TestFloatParameter()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<FloatTest.NamedSingle, float>(GenericType<FloatTest.NamedSingle>.Class, "3.5");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<FloatTest>();
            float x = 3.5F;
            o.Verify(x);
        }
    }

    public class StringTest
    {
        private readonly string str;

        [Inject]
        public StringTest([Parameter(typeof(NamedString))] string s)
        {
            this.str = s;
        }

        public void Verify(string s)
        {
            Assert.Equal(s, str);
        }

        [NamedParameter(DefaultValue = " ")]
        public class NamedString : Name<string>
        {
        }
    }

    public class CharTest
    {
        private readonly char c;

        [Inject]
        public CharTest([Parameter(typeof(NamedChar))] char c)
        {
            this.c = c;
        }

        public void Verify(char s)
        {
            Assert.Equal(s, c);
        }

        [NamedParameter(DefaultValue = " ")]
        public class NamedChar : Name<char>
        {            
        }
    }

    public class ByteTest
    {
        private readonly byte b;

        [Inject]
        public ByteTest([Parameter(typeof(NamedByte))] byte b)
        {
            this.b = b;
        }

        public void Verify(byte v)
        {
            Assert.Equal(v, b);
        }

        [NamedParameter(DefaultValue = "7")]
        public class NamedByte : Name<byte>
        {
        }
    }

    public class BooleanTest
    {
        private readonly bool b;

        [Inject]
        public BooleanTest([Parameter(typeof(NamedBool))] bool b)
        {
            this.b = b;
        }

        public void Verify(bool v)
        {
            Assert.Equal(v, b);
        }

        [NamedParameter(DefaultValue = "false")]
        public class NamedBool : Name<bool>
        {
        }
    }

    public class ByteArrayTest
    {
        private readonly byte[] b;

        [Inject]
        public ByteArrayTest([Parameter(typeof(NamedByteArray))] byte[] b)
        {
            this.b = b;
        }

        public bool Verify(byte[] v)
        {
            if (v.Length != b.Length)
            {
                return false;
            }

            for (int i = 0; i < v.Length; i++)
            {   
                if (v[i] != b[i])
                {
                    return false;
                }
            }

            return true;
        }

        [NamedParameter]
        public class NamedByteArray : Name<byte[]>
        {
        }
    }

    public class Int16Test
    {
        private readonly short s;

        [Inject]
        public Int16Test([Parameter(typeof(NamedShort))] short s)
        {
            this.s = s;
        }

        public void Verify(short v)
        {
            Assert.Equal(v, s);
        }

        [NamedParameter(DefaultValue = "3")]
        public class NamedShort : Name<short>
        {
        }
    }

    public class Int32Test
    {
        private readonly int i;

        [Inject]
        public Int32Test([Parameter(typeof(NamedInt))] int i)
        {
            this.i = i;
        }

        public void Verify(int v)
        {
            Assert.Equal(v, i);
        }

        [NamedParameter(DefaultValue = "3")]
        public class NamedInt : Name<int>
        {
        }
    }

    public class Int64Test
    {
        private readonly long l;

        [Inject]
        public Int64Test([Parameter(typeof(NamedLong))] long l)
        {
            this.l = l;
        }

        public void Verify(int v)
        {
            Assert.Equal(v, l);
        }

        [NamedParameter(DefaultValue = "34567")]
        public class NamedLong : Name<long>
        {
        }
    }

    public class FloatTest
    {
        private readonly float f;

        [Inject]
        public FloatTest([Parameter(typeof(NamedSingle))] float f)
        {
            this.f = f;
        }

        public void Verify(float v)
        {
            Assert.Equal(v, f);
        }

        [NamedParameter(DefaultValue = "12.5")]
        public class NamedSingle : Name<float>
        {
        }
    }
}
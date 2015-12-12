/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.ClassHierarchy;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.ClassHierarchy
{
    public class TestParameterParser
    {
        [Fact]
        public void ParseIntTest()
        {
            var parser = new ParameterParser();
            int o = (int)parser.Parse(typeof(int), "4");
        }

        [Fact]
        public void ParseBoolTest()
        {
            var parser = new ParameterParser();
            bool o = (bool)parser.Parse(typeof(bool), "false");
        }

        [Fact]
        public void ParseLongTest()
        {
            var parser = new ParameterParser();
            long o = (long)parser.Parse(typeof(long), "8675309");
        }

        [Fact]
        public void ParseStringTest()
        {
            var parser = new ParameterParser();
            string o = (string)parser.Parse(typeof(string), "hello");
        }

        [Fact]
        public void ParseDoubleTest()
        {
            var parser = new ParameterParser();
            double o = (double)parser.Parse(typeof(double), "12.6");
        }

        [Fact]
        public void ParseCharTest()
        {
            var parser = new ParameterParser();
            char o = (char)parser.Parse(typeof(char), "c");
        }

        [Fact]
        public void ParseByteTest()
        {
            var parser = new ParameterParser();
            byte o = (byte)parser.Parse(typeof(byte), "8");
        }

        [Fact]
        public void ParseShortTest()
        {
            var parser = new ParameterParser();
            short o = (short)parser.Parse(typeof(short), "8");
        }

        [Fact]
        public void ParseFloatTest()
        {
            var parser = new ParameterParser();
            float o = (float)parser.Parse(typeof(float), "8.567");
        }

        [Fact]
        public void ParseByteArrayTest()
        {
            var parser = new ParameterParser();
            byte[] o = (byte[])parser.Parse(typeof(byte[]), "hello");
        }

        [Fact]
        public void ParameterParserTest()
        {
            ParameterParser p = new ParameterParser();
            p.AddParser(typeof(FooParser));
            Foo f = (Foo)p.Parse(typeof(Foo), "woot");
            Assert.Equal(f.s, "woot");
        }
        
        [Fact]
        public void TestUnregisteredParameterParser() 
        {
            ParameterParser p = new ParameterParser();
            
            // p.AddParser(typeof(FooParser));
            Foo f = null;
            try
            {
                f = (Foo)p.Parse(typeof(Foo), "woot");
            }
            catch (NotSupportedException)
            {
            }
            Assert.Null(f);           
        }

       [Fact]
        public void TestReturnSubclass() 
       {
            ParameterParser p = new ParameterParser();
            p.AddParser(typeof(BarParser));
            Bar f = (Bar)p.Parse(typeof(Foo), "woot");
            Assert.Equal(f.s, "woot");    
        }

        [Fact]
        public void TestGoodMerge()
        {
            ParameterParser old = new ParameterParser();
            old.AddParser(typeof(BarParser));
            ParameterParser nw = new ParameterParser();
            nw.MergeIn(old);
            Bar f = (Bar)nw.Parse(typeof(Foo), "woot");
            Assert.Equal(f.s, "woot");   
        }

        [Fact]
        public void TestGoodMerge2()
        {
            ParameterParser old = new ParameterParser();
            old.AddParser(typeof(BarParser));
            ParameterParser nw = new ParameterParser();
            nw.AddParser(typeof(BarParser));
            nw.MergeIn(old);
            Bar f = (Bar)nw.Parse(typeof(Foo), "woot");
            Assert.Equal(f.s, "woot");   
        }

        [Fact]
        public void TestBadMerge()
        {
            string msg = null;
            try
            {
                ParameterParser old = new ParameterParser();
                old.AddParser(typeof(BarParser));
                ParameterParser nw = new ParameterParser();
                nw.AddParser(typeof(FooParser));
                nw.MergeIn(old);
                msg = "Conflict detected when merging parameter parsers! To parse org.apache.reef.tang.implementation.java.TestParameterParser$Foo I have a: TestParameterParser$FooParser the other instance has a: TestParameterParser$BarParser";
            }
            catch (ArgumentException)
            {
            }
            Assert.Null(msg);
        }

        [Fact]
        public void testEndToEnd() 
        {
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new Type[] { typeof(BarParser) });
            cb.BindNamedParameter<SomeNamedFoo, Foo>(GenericType<SomeNamedFoo>.Class, "hdfs://woot");
            ILikeBars ilb = tang.NewInjector(cb.Build()).GetInstance<ILikeBars>();
            Assert.NotNull(ilb);
        }

        [Fact]
        public void TestDelegatingParser()
        {
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { }, new IConfiguration[] { }, new Type[] { typeof(TypeParser) });

            ICsConfigurationBuilder cb2 = tang.NewConfigurationBuilder(new IConfiguration[] { cb.Build() });

            cb2.BindNamedParameter<ParseName, ParseableType>(GenericType<ParseName>.Class, "a"); // ParseName : Name<ParseableType>

            ParseableType t = (ParseableType)tang.NewInjector(cb2.Build()).GetNamedInstance(typeof(ParseName));
            Assert.True(t is ParseTypeA);

            cb2 = tang.NewConfigurationBuilder(cb.Build());
            cb2.BindNamedParameter<ParseNameB, ParseTypeB>(GenericType<ParseNameB>.Class, "b"); // ParseNameB : Name<ParseTypeB : ParseableType>
            cb2.BindNamedParameter<ParseNameA, ParseableType>(GenericType<ParseNameA>.Class, "a"); // ParseNameA : Name<ParseableType>

            tang.NewInjector(cb2.Build()).GetInstance(typeof(NeedsA));
            tang.NewInjector(cb2.Build()).GetInstance(typeof(NeedsB));
        }

        class FooParser : IExternalConstructor<Foo>
        {
            private readonly Foo foo;
            [Inject]
            public FooParser(string s)
            {
                this.foo = new Foo(s);
            }

            public Foo NewInstance()
            {
                return foo;
            }
        }

        class BarParser : IExternalConstructor<Foo>
        {
            private readonly Bar bar;
            [Inject]
            public BarParser(string s)
            {
                this.bar = new Bar(s);
            }
            public Foo NewInstance()
            {
                return bar;
            }
        }

        class Foo
        {
            public readonly string s;
            public Foo(string s) 
            { 
                this.s = s; 
            }
        }
        class Bar : Foo
        {
            public Bar(string s) : base(s) 
            { 
            }
        }

        [NamedParameter]
        class SomeNamedFoo : Name<Foo> 
        { 
        }

        class ILikeBars
        {
            [Inject]
            ILikeBars([Parameter(typeof(SomeNamedFoo))] Foo bar)
            {
                Bar b = (Bar)bar;
                Assert.Equal(b.s, "hdfs://woot");
            }
        }

        class ParseableType
        {
        }

        class ParseTypeA : ParseableType
        {
        }

        class ParseTypeB : ParseableType
        {
        }

        class TypeParser : IExternalConstructor<ParseableType>
        {
            readonly ParseableType instance;
            [Inject]
            public TypeParser(string s)
            {
                if (s.Equals("a")) 
                { 
                    instance = new ParseTypeA(); 
                }
                if (s.Equals("b")) 
                { 
                    instance = new ParseTypeB(); 
                }
            }

            public ParseableType NewInstance()
            {
                return instance;
            }
        }

        [NamedParameter]
        class ParseName : Name<ParseableType>
        {
        }

        [NamedParameter]
        class ParseNameA : Name<ParseableType>
        {
        }

        [NamedParameter]
        class ParseNameB : Name<ParseTypeB>
        {
        }

        class NeedsA
        {
            [Inject]
            public NeedsA([Parameter(typeof(ParseNameA))] ParseableType a)
            {
                Assert.True(a is ParseTypeA);
            }
        }

        class NeedsB
        {
            [Inject]
            public NeedsB([Parameter(typeof(ParseNameB))] ParseTypeB b)
            {
                Assert.True(b is ParseTypeB);
            }
        }
    }
}
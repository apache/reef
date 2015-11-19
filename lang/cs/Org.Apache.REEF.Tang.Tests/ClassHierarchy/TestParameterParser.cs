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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.ClassHierarchy;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Tests.ClassHierarchy
{
    [TestClass]
    public class TestParameterParser
    {
        [TestMethod]
        public void ParseIntTest()
        {
            var parser = new ParameterParser();
            Int32 o = (Int32)parser.Parse(typeof(Int32), "4");

        }

        [TestMethod]
        public void ParseBoolTest()
        {
            var parser = new ParameterParser();
            Boolean o = (Boolean)parser.Parse(typeof(Boolean), "false");
        }

        [TestMethod]
        public void ParseLongTest()
        {
            var parser = new ParameterParser();
            long o = (long)parser.Parse(typeof(long), "8675309");
        }

        [TestMethod]
        public void ParseStringTest()
        {
            var parser = new ParameterParser();
            string o = (string)parser.Parse(typeof(string), "hello");
        }

        [TestMethod]
        public void ParseDoubleTest()
        {
            var parser = new ParameterParser();
            Double o = (Double)parser.Parse(typeof(double), "12.6");
        }

        [TestMethod]
        public void ParseCharTest()
        {
            var parser = new ParameterParser();
            Char o = (Char)parser.Parse(typeof(char), "c");
        }

        [TestMethod]
        public void ParseByteTest()
        {
            var parser = new ParameterParser();
            Byte o = (Byte)parser.Parse(typeof(byte), "8");
        }

        [TestMethod]
        public void ParseShortTest()
        {
            var parser = new ParameterParser();
            Int16 o = (Int16)parser.Parse(typeof(short), "8");
        }

        [TestMethod]
        public void ParseFloatTest()
        {
            var parser = new ParameterParser();
            Single o = (Single)parser.Parse(typeof(float), "8.567");
        }

        [TestMethod]
        public void ParseByteArrayTest()
        {
            var parser = new ParameterParser();
            byte[] o = (byte[])parser.Parse(typeof(byte[]), "hello");
        }

        [TestMethod]
        public void ParameterParserTest()
        {
            ParameterParser p = new ParameterParser();
            p.AddParser(typeof(FooParser));
            Foo f = (Foo)p.Parse(typeof(Foo), "woot");
            Assert.AreEqual(f.s, "woot");
        }
        
        [TestMethod]
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
            Assert.IsNull(f);           
        }

       [TestMethod]
        public void TestReturnSubclass() 
       {
            ParameterParser p = new ParameterParser();
            p.AddParser(typeof(BarParser));
            Bar f = (Bar)p.Parse(typeof(Foo), "woot");
            Assert.AreEqual(f.s, "woot");    
        }

        [TestMethod]
        public void TestGoodMerge()
        {
            ParameterParser old = new ParameterParser();
            old.AddParser(typeof(BarParser));
            ParameterParser nw = new ParameterParser();
            nw.MergeIn(old);
            Bar f = (Bar)nw.Parse(typeof(Foo), "woot");
            Assert.AreEqual(f.s, "woot");   
        }

        [TestMethod]
        public void TestGoodMerge2()
        {
            ParameterParser old = new ParameterParser();
            old.AddParser(typeof(BarParser));
            ParameterParser nw = new ParameterParser();
            nw.AddParser(typeof(BarParser));
            nw.MergeIn(old);
            Bar f = (Bar)nw.Parse(typeof(Foo), "woot");
            Assert.AreEqual(f.s, "woot");   
        }

        [TestMethod]
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
            Assert.IsNull(msg);
        }

        [TestMethod]
        public void testEndToEnd() 
        {
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new Type[] { typeof(BarParser) });
            cb.BindNamedParameter<SomeNamedFoo, Foo>(GenericType<SomeNamedFoo>.Class, "hdfs://woot");
            ILikeBars ilb = tang.NewInjector(cb.Build()).GetInstance<ILikeBars>();
            Assert.IsNotNull(ilb);
        }

        [TestMethod]
        public void TestDelegatingParser()
        {
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { }, new IConfiguration[] { }, new Type[] { typeof(TypeParser) });

            ICsConfigurationBuilder cb2 = tang.NewConfigurationBuilder(new IConfiguration[] { cb.Build() });

            cb2.BindNamedParameter<ParseName, ParseableType>(GenericType<ParseName>.Class, "a"); // ParseName : Name<ParseableType>

            ParseableType t = (ParseableType)tang.NewInjector(cb2.Build()).GetNamedInstance(typeof(ParseName));
            Assert.IsTrue(t is ParseTypeA);

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
            public BarParser(String s)
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
            public Foo(string s) { this.s = s; }
        }
        class Bar : Foo
        {
            public Bar(string s) : base(s) { }
        }

        [NamedParameter]
        class SomeNamedFoo : Name<Foo> { }

        class ILikeBars
        {
            [Inject]
            ILikeBars([Parameter(typeof(SomeNamedFoo))] Foo bar)
            {
                Bar b = (Bar)bar;
                Assert.AreEqual(b.s, "hdfs://woot");
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
            public TypeParser(String s)
            {
                if (s.Equals("a")) { instance = new ParseTypeA(); }
                if (s.Equals("b")) { instance = new ParseTypeB(); }
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
                Assert.IsTrue(a is ParseTypeA);
            }
        }

        class NeedsB
        {
            [Inject]
            public NeedsB([Parameter(typeof(ParseNameB))] ParseTypeB b)
            {
                Assert.IsTrue(b is ParseTypeB);
            }
        }
    }
}
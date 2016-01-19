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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.Injection
{
    /// <summary>
    /// Test injection with a List
    /// </summary>
    public class TestListInjection
    {
        /// <summary>
        /// Tests the string inject default.
        /// </summary>
        [Fact]
        public void TestStringInjectDefault()
        {
            StringClass b = TangFactory.GetTang().NewInjector().GetInstance<StringClass>();

            IList<string> actual = b.StringList;

            Assert.True(actual.Contains("one"));
            Assert.True(actual.Contains("two"));
            Assert.True(actual.Contains("three"));
        }

        /// <summary>
        /// Tests the int inject default.
        /// </summary>
        [Fact]
        public void TestIntInjectDefault()
        {
            IntClass b = TangFactory.GetTang().NewInjector().GetInstance<IntClass>();

            IList<int> actual = b.IntList;

            Assert.True(actual.Contains(1));
            Assert.True(actual.Contains(2));
            Assert.True(actual.Contains(3));
        }

        /// <summary>
        /// Tests the string inject configuration builder.
        /// </summary>
        [Fact]
        public void TestStringInjectConfigurationBuilder()
        {
            ICsClassHierarchy classH = TangFactory.GetTang().GetDefaultClassHierarchy();
            INamedParameterNode np = (INamedParameterNode)classH.GetNode(typeof(StringList));
            IList<string> injected = new List<string>();
            injected.Add("hi");
            injected.Add("hello");
            injected.Add("bye");

            IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList(np, injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IList<string> actual = ((StringClass)i.GetInstance(typeof(StringClass))).StringList;

            Assert.True(actual.Contains("hi"));
            Assert.True(actual.Contains("hello"));
            Assert.True(actual.Contains("bye"));
            Assert.Equal(actual.Count, 3);
        }

        /// <summary>
        /// Tests the bool list with named parameter.
        /// </summary>
        [Fact]
        public void TestBoolListWithNamedParameter()
        {
            ICsClassHierarchy classH = TangFactory.GetTang().GetDefaultClassHierarchy();
            INamedParameterNode np = (INamedParameterNode)classH.GetNode(typeof(BoolListClass.NamedBoolList));
            IList<string> injected = new List<string>();
            injected.Add("true");
            injected.Add("false");
            injected.Add("true");

            IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList(np, injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            BoolListClass o = i.GetInstance<BoolListClass>();

            IList<bool> expected = new List<bool>();
            expected.Add(true);
            expected.Add(false);
            expected.Add(true);
            o.Verify(expected);
        }

        /// <summary>
        /// Tests the type of the bool list with generic.
        /// </summary>
        [Fact]
        public void TestBoolListWithGenericType()
        {
            IList<string> injected = new List<string>();
            injected.Add("true");
            injected.Add("false");
            injected.Add("true");

            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList<BoolListClass.NamedBoolList, bool>(GenericType<BoolListClass.NamedBoolList>.Class, injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            BoolListClass o = i.GetInstance<BoolListClass>();

            IList<bool> expected = new List<bool>();
            expected.Add(true);
            expected.Add(false);
            expected.Add(true);
            o.Verify(expected);
        }

        /// <summary>
        /// Tests the int list with named parameter.
        /// </summary>
        [Fact]
        public void TestIntListWithNamedParameter()
        {
            ICsClassHierarchy classH = TangFactory.GetTang().GetDefaultClassHierarchy();
            INamedParameterNode np = (INamedParameterNode)classH.GetNode(typeof(IntListClass.NamedIntList));
            IList<string> injected = new List<string>();
            injected.Add("1");
            injected.Add("2");
            injected.Add("3");

            IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList(np, injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IntListClass o = i.GetInstance<IntListClass>();

            IList<int> expected = new List<int>();
            expected.Add(1);
            expected.Add(2);
            expected.Add(3);
            o.Verify(expected);
        }

        /// <summary>
        /// Tests the type of the int list with generic.
        /// </summary>
        [Fact]
        public void TestIntListWithGenericType()
        {
            IList<string> injected = new List<string>();
            injected.Add("1");
            injected.Add("2");
            injected.Add("3");

            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList<IntListClass.NamedIntList, int>(GenericType<IntListClass.NamedIntList>.Class, injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IntListClass o = i.GetInstance<IntListClass>();

            IList<int> expected = new List<int>();
            expected.Add(1);
            expected.Add(2);
            expected.Add(3);
            o.Verify(expected);
        }

        /// <summary>
        /// Tests the string inject.
        /// </summary>
        [Fact]
        public void TestStringInject()
        {
            IList<string> injected = new List<string>();
            injected.Add("hi");
            injected.Add("hello");
            injected.Add("bye");

            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList<StringList, string>(GenericType<StringList>.Class, injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IList<string> actual = ((StringClass)i.GetInstance(typeof(StringClass))).StringList;

            Assert.True(actual.Contains("hi"));
            Assert.True(actual.Contains("hello"));
            Assert.True(actual.Contains("bye"));
            Assert.Equal(actual.Count, 3);
        }

        /// <summary>
        /// Tests the node inject and bind volatile instance.
        /// </summary>
        [Fact]
        public void TestNodeInjectAndBindVolatileInstance()
        {
            ICsClassHierarchy classH = TangFactory.GetTang().GetDefaultClassHierarchy();
            INamedParameterNode np = (INamedParameterNode)classH.GetNode(typeof(ListOfClasses));
            IList<INode> injected = new List<INode>();
            injected.Add(classH.GetNode(typeof(TestSetInjection.Integer)));
            injected.Add(classH.GetNode(typeof(TestSetInjection.Float)));

            IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList(np, injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            i.BindVolatileInstance(GenericType<TestSetInjection.Integer>.Class, new TestSetInjection.Integer(42));
            i.BindVolatileInstance(GenericType<TestSetInjection.Float>.Class, new TestSetInjection.Float(42.0001f));
            IList<INumber> actual = ((PoolListClass)i.GetInstance(typeof(PoolListClass))).Numbers;

            Assert.True(actual.Contains(new TestSetInjection.Integer(42)));
            Assert.True(actual.Contains(new TestSetInjection.Float(42.0001f)));
        }

        /// <summary>
        /// Tests the class name inject with named parameter node.
        /// </summary>
        [Fact]
        public void TestClassNameInjectWithNamedParameterNode()
        {
            ICsClassHierarchy classH = TangFactory.GetTang().GetDefaultClassHierarchy();
            INamedParameterNode np = (INamedParameterNode)classH.GetNode(typeof(ListOfClasses));

            IList<string> injected = new List<string>();
            injected.Add(typeof(TestSetInjection.Integer).AssemblyQualifiedName);
            injected.Add(typeof(TestSetInjection.Float).AssemblyQualifiedName);

            IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList(np, injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            i.BindVolatileInstance(GenericType<TestSetInjection.Integer>.Class, new TestSetInjection.Integer(42));
            i.BindVolatileInstance(GenericType<TestSetInjection.Float>.Class, new TestSetInjection.Float(42.0001f));
            IList<INumber> actual = ((PoolListClass)i.GetInstance(typeof(PoolListClass))).Numbers;

            Assert.True(actual.Contains(new TestSetInjection.Integer(42)));
            Assert.True(actual.Contains(new TestSetInjection.Float(42.0001f)));
        }

        /// <summary>
        /// Tests the name of the class name inject with named parameter.
        /// </summary>
        [Fact]
        public void TestClassNameInjectWithNamedParameterName()
        {
            IList<string> injected = new List<string>();
            injected.Add(typeof(TestSetInjection.Integer).AssemblyQualifiedName);
            injected.Add(typeof(TestSetInjection.Float).AssemblyQualifiedName);

            IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList(typeof(ListOfClasses).AssemblyQualifiedName, injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            i.BindVolatileInstance(GenericType<TestSetInjection.Integer>.Class, new TestSetInjection.Integer(42));
            i.BindVolatileInstance(GenericType<TestSetInjection.Float>.Class, new TestSetInjection.Float(42.0001f));
            IList<INumber> actual = ((PoolListClass)i.GetInstance(typeof(PoolListClass))).Numbers;

            Assert.True(actual.Contains(new TestSetInjection.Integer(42)));
            Assert.True(actual.Contains(new TestSetInjection.Float(42.0001f)));
        }

        /// <summary>
        /// Tests the object inject with injectable subclasses.
        /// </summary>
        [Fact]
        public void TestObjectInjectWithInjectableSubclasses()
        {
            IList<string> injected = new List<string>();
            injected.Add(typeof(TestSetInjection.Integer1).AssemblyQualifiedName);
            injected.Add(typeof(TestSetInjection.Integer2).AssemblyQualifiedName);
            injected.Add(typeof(TestSetInjection.Integer3).AssemblyQualifiedName);

            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList(typeof(ListOfClasses).AssemblyQualifiedName, injected);
            cb.BindNamedParameter<TestSetInjection.Integer1.NamedInt, int>(GenericType<TestSetInjection.Integer1.NamedInt>.Class, "5");
            cb.BindNamedParameter<TestSetInjection.Integer3.NamedInt, int>(GenericType<TestSetInjection.Integer3.NamedInt>.Class, "10");

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            
            IList<INumber> actual = ((PoolListClass)i.GetInstance(typeof(PoolListClass))).Numbers;

            Assert.True(actual.Count == 3);
            Assert.True(actual.Contains(new TestSetInjection.Integer1(5)));
            Assert.True(actual.Contains(new TestSetInjection.Integer2()));
            Assert.True(actual.Contains(new TestSetInjection.Integer3(10)));
        }

        /// <summary>
        /// Tests the object inject with injectable subclasses multiple instances.
        /// </summary>
        [Fact]
        public void TestObjectInjectWithInjectableSubclassesMultipleInstances()
        {
            IList<string> injected = new List<string>();
            injected.Add(typeof(TestSetInjection.Integer1).AssemblyQualifiedName);
            injected.Add(typeof(TestSetInjection.Integer1).AssemblyQualifiedName);
            injected.Add(typeof(TestSetInjection.Float1).AssemblyQualifiedName);

            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<TestSetInjection.Integer1.NamedInt, int>(GenericType<TestSetInjection.Integer1.NamedInt>.Class, "5");
            cb.BindNamedParameter<TestSetInjection.Float1.NamedFloat, float>(GenericType<TestSetInjection.Float1.NamedFloat>.Class, "12.5");
            cb.BindList<ListOfClasses, INumber>(GenericType<ListOfClasses>.Class, injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());

            IList<INumber> actual = ((PoolListClass)i.GetInstance(typeof(PoolListClass))).Numbers;

            Assert.True(actual.Count == 3);
            Assert.True(actual.Contains(new TestSetInjection.Integer1(5)));
            Assert.True(actual.Contains(new TestSetInjection.Integer1(5)));
            Assert.True(actual.Contains(new TestSetInjection.Float1(12.5f)));
        }

        /// <summary>
        /// Tests the object inject with injectable subclasses and typeof named parameter.
        /// </summary>
        [Fact]
        public void TestObjectInjectWithInjectableSubclassesAndTypeofNamedParameter()
        {
            IList<string> injected = new List<string>();
            injected.Add(typeof(TestSetInjection.Integer1).AssemblyQualifiedName);
            injected.Add(typeof(TestSetInjection.Integer2).AssemblyQualifiedName);
            injected.Add(typeof(TestSetInjection.Integer3).AssemblyQualifiedName);

            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<TestSetInjection.Integer1.NamedInt, int>(GenericType<TestSetInjection.Integer1.NamedInt>.Class, "5");
            cb.BindNamedParameter<TestSetInjection.Integer3.NamedInt, int>(GenericType<TestSetInjection.Integer3.NamedInt>.Class, "10");
            cb.BindList<ListOfClasses, INumber>(GenericType<ListOfClasses>.Class, injected);
          
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());

            IList<INumber> actual = ((PoolListClass)i.GetInstance(typeof(PoolListClass))).Numbers;

            Assert.True(actual.Count == 3);
            Assert.True(actual.Contains(new TestSetInjection.Integer1(5)));
            Assert.True(actual.Contains(new TestSetInjection.Integer2()));
            Assert.True(actual.Contains(new TestSetInjection.Integer3(10)));
        }

        /// <summary>
        /// Tests the object inject with names configuration builder.
        /// </summary>
        [Fact]
        public void TestObjectInjectWithNames()
        {
            ICsClassHierarchy classH = TangFactory.GetTang().GetDefaultClassHierarchy();
            IList<INode> injected = new List<INode>();
            injected.Add(classH.GetNode(typeof(TestSetInjection.Integer)));
            injected.Add(classH.GetNode(typeof(TestSetInjection.Float)));

            IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList(typeof(ListOfClasses).AssemblyQualifiedName, injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            i.BindVolatileInstance(GenericType<TestSetInjection.Integer>.Class, new TestSetInjection.Integer(42));
            i.BindVolatileInstance(GenericType<TestSetInjection.Float>.Class, new TestSetInjection.Float(42.0001f));
            IList<INumber> actual = ((PoolListClass)i.GetInstance(typeof(PoolListClass))).Numbers;

            Assert.True(actual.Contains(new TestSetInjection.Integer(42)));
            Assert.True(actual.Contains(new TestSetInjection.Float(42.0001f)));
        }

        /// <summary>
        /// Tests the object inject with type type cs configuration builder.
        /// </summary>
        [Fact]
        public void TestObjectInjectWithTypeType()
        {
            IList<Type> injected = new List<Type>();
            injected.Add(typeof(TestSetInjection.Integer));
            injected.Add(typeof(TestSetInjection.Float));

            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList(typeof(ListOfClasses), injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            i.BindVolatileInstance(GenericType<TestSetInjection.Integer>.Class, new TestSetInjection.Integer(42));
            i.BindVolatileInstance(GenericType<TestSetInjection.Float>.Class, new TestSetInjection.Float(42.0001f));
            IList<INumber> actual = ((PoolListClass)i.GetInstance(typeof(PoolListClass))).Numbers;

            Assert.True(actual.Contains(new TestSetInjection.Integer(42)));
            Assert.True(actual.Contains(new TestSetInjection.Float(42.0001f)));
        }

        ////<summary>
        ////Tests the subclass inject with multiple instances.
        ////</summary>
        ////[Fact] 
        ////public void TestSubclassInjectWithMultipleInstances()
        ////{
        ////    ICsConfigurationBuilder cb1 = TangFactory.GetTang().NewConfigurationBuilder()
        ////        .BindNamedParameter<TestSetInjection.Integer3.NamedInt, int>(GenericType<TestSetInjection.Integer3.NamedInt>.Class, "10");
        ////    TestSetInjection.Integer3 integer3 = TangFactory.GetTang().NewInjector(cb1.Build()).GetInstance<TestSetInjection.Integer3>();

        ////    ICsClassHierarchy classH = TangFactory.GetTang().GetDefaultClassHierarchy();
        ////    INamedParameterNode np = (INamedParameterNode)classH.GetNode(typeof(ListOfClasses));
        ////    IList<object> injected = new List<object>();
        ////    injected.Add(new TestSetInjection.Integer1(42)); //instance from the same class
        ////    injected.Add(new TestSetInjection.Integer1(30)); //instance from the same class
        ////    injected.Add(new TestSetInjection.Float1(42.0001f)); //instance from another subclass of the same interface
        ////    injected.Add(typeof(TestSetInjection.Float1).AssemblyQualifiedName); //inject from another subclass of the same interface
        ////    injected.Add(typeof(TestSetInjection.Integer1).AssemblyQualifiedName); //inject using configuration
        ////    injected.Add(typeof(TestSetInjection.Integer2).AssemblyQualifiedName); //inject using default
        ////    injected.Add(integer3); //add pre injected instance

        ////    ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
        ////    cb.BindNamedParameter<TestSetInjection.Integer1.NamedInt, int>(GenericType<TestSetInjection.Integer1.NamedInt>.Class, "5");
        ////    cb.BindNamedParameter<TestSetInjection.Float1.NamedFloat, float>(GenericType<TestSetInjection.Float1.NamedFloat>.Class, "12.5");
        ////    cb.BindList(np, injected);

        ////    IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
        ////    IList<INumber> actual = ((PoolListClass)i.GetInstance(typeof(PoolListClass))).Numbers;
        ////    Assert.True(actual.Count == 7);
        ////    Assert.True(actual.Contains(new TestSetInjection.Integer1(42)));
        ////    Assert.True(actual.Contains(new TestSetInjection.Integer1(30)));
        ////    Assert.True(actual.Contains(new TestSetInjection.Float1(42.0001f)));
        ////    Assert.True(actual.Contains(new TestSetInjection.Float1(12.5f)));
        ////    Assert.True(actual.Contains(new TestSetInjection.Integer1(5)));
        ////    Assert.True(actual.Contains(new TestSetInjection.Integer2()));
        ////    Assert.True(actual.Contains(new TestSetInjection.Integer3(10)));
        ////}
    }

    [NamedParameter(DefaultValues = new string[] { "one", "two", "three" })]
    internal class StringList : Name<IList<string>>
    {
    }

    [NamedParameter(DefaultValues = new string[] { "1", "2", "3" })]
    internal class IntList : Name<IList<int>>
    {
    }

    [NamedParameter(DefaultValues = new string[] { "1", "2", "3" })]
    internal class IntegerList : Name<IList<TestSetInjection.Integer>>
    {
    }

    internal class StringClass
    {
        [Inject]
        private StringClass([Parameter(typeof(StringList))] IList<string> stringList)
        {
            this.StringList = stringList;
        }

        public IList<string> StringList { get; set; }
    }

    internal class IntClass
    {
        [Inject]
        private IntClass([Parameter(typeof(IntList))] IList<int> integerList)
        {
            this.IntList = integerList;
        }
    
        public IList<int> IntList { get; set; }
    }

    internal class IntegerListClass
    {
        [Inject]
        private IntegerListClass([Parameter(typeof(IntegerList))] IList<TestSetInjection.Integer> integerList)
        {
            this.IntegerList = integerList;
        }

        public IList<TestSetInjection.Integer> IntegerList { get; set; }
    }

    [NamedParameter(DefaultClasses = new Type[] { typeof(TestSetInjection.Integer), typeof(TestSetInjection.Float) })]
    internal class ListOfClasses : Name<IList<INumber>>
    {
    }

    internal class PoolListClass
    {
        [Inject]
        private PoolListClass([Parameter(typeof(ListOfClasses))] IList<INumber> numbers)
        {
            this.Numbers = numbers;
        }

        public IList<INumber> Numbers { get; set; }
    }

    internal class BoolListClass
    {
        private readonly IList<bool> b;

        [Inject]
        public BoolListClass([Parameter(typeof(NamedBoolList))] IList<bool> b)
        {
            this.b = b;
        }

        public void Verify(IList<bool> v)
        {
            Assert.Equal(v.Count, b.Count);
            foreach (bool bv in v)
            {
                Assert.True(b.Contains(bv));
            }
        }

        [NamedParameter]
        public class NamedBoolList : Name<IList<bool>>
        {
        }
    }

    internal class IntListClass
    {
        private readonly IList<int> l;

        [Inject]
        public IntListClass([Parameter(typeof(NamedIntList))] IList<int> b)
        {
            this.l = b;
        }

        public void Verify(IList<int> v)
        {
            Assert.Equal(v.Count, l.Count);
            foreach (int iv in v)
            {
                Assert.True(l.Contains(iv));
            }
        }

        [NamedParameter]
        public class NamedIntList : Name<IList<int>>
        {
        }
    }
}

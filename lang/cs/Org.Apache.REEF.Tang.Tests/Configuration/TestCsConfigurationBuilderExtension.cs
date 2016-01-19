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

using System.Collections.Generic;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Tests.Injection;
using Org.Apache.REEF.Tang.Tests.Tang;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.Configuration
{
    /// <summary>
    /// This class is to test extension API defined in ICsConfigurationBuilder
    /// </summary>
    public class TestCsConfigurationBuilderExtension
    {
        [Fact]
        public void TestBindNamedParameter1()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<AImplName, Aimpl, INamedImplA>();
            cb.BindNamedParameter<BImplName, Bimpl, INamedImplA>();

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            Aimpl a1 = (Aimpl)i.GetNamedInstance<AImplName, INamedImplA>(GenericType<AImplName>.Class);
            Aimpl a2 = (Aimpl)i.GetNamedInstance<AImplName, INamedImplA>(GenericType<AImplName>.Class);
            Bimpl b1 = (Bimpl)i.GetNamedInstance<BImplName, INamedImplA>(GenericType<BImplName>.Class);
            Bimpl b2 = (Bimpl)i.GetNamedInstance<BImplName, INamedImplA>(GenericType<BImplName>.Class);
            Assert.Same(a1, a2);
            Assert.Same(b1, b2);
        }

        [Fact]
        public void TestBindStringNamedParam()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindStringNamedParam<StringTest.NamedString>("foo");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<StringTest>();
            o.Verify("foo");
        }

        [Fact]
        public void TestBindIntNamedParam()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindIntNamedParam<Int32Test.NamedInt>("8");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<Int32Test>();
            o.Verify(8);
        }

        [Fact]
        public void TestBindNamedParam()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParam<BooleanTest.NamedBool, bool>("true");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<BooleanTest>();
            o.Verify(true);
        }

        [Fact]
        public void TestBindSetEntryImplValue()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindSetEntry<TestSetInjection.SetOfClasses, TestSetInjection.Integer1, INumber>()  // bind an impl to the interface of the set
              .BindIntNamedParam<TestSetInjection.Integer1.NamedInt>("4"); // bind parameter for the impl

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());

            ISet<INumber> actual = i.GetInstance<TestSetInjection.Pool>().Numbers;
            ISet<INumber> expected = new HashSet<INumber>();
            expected.Add(new TestSetInjection.Integer1(4));

            Assert.True(Utilities.Utilities.Equals<INumber>(actual, expected));
        }

        [Fact]
        public void TestBindSetEntryStringValue()
        {
            IConfiguration conf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<SetOfNumbers, string>("four")
                .BindSetEntry<SetOfNumbers, string>("five")
                .BindSetEntry<SetOfNumbers, string>("six")
                .Build();

            Box b = (Box)TangFactory.GetTang().NewInjector(conf).GetInstance(typeof(Box));
            ISet<string> actual = b.Numbers;

            Assert.True(actual.Contains("four"));
            Assert.True(actual.Contains("five"));
            Assert.True(actual.Contains("six"));
        }

        [Fact]
        public void TestBindImplementation()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation<Interf, Impl>();
            Interf o = TangFactory.GetTang().NewInjector(cb.Build()).GetInstance<Interf>();
            Assert.True(o is Impl);
        }

        [Fact]
        public void TestBindList()
        {
            IList<string> injected = new List<string>();
            injected.Add("hi");
            injected.Add("hello");
            injected.Add("bye");

            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindList<StringList, string>(injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IList<string> actual = ((StringClass)i.GetInstance(typeof(StringClass))).StringList;

            Assert.True(actual.Contains("hi"));
            Assert.True(actual.Contains("hello"));
            Assert.True(actual.Contains("bye"));
            Assert.Equal(actual.Count, 3);
        }

        [Fact]
        public void TestObjectInjectWithInjectableSubclassesMultipleInstances()
        {
            IList<string> injected = new List<string>();
            injected.Add(typeof(TestSetInjection.Integer1).AssemblyQualifiedName);
            injected.Add(typeof(TestSetInjection.Integer1).AssemblyQualifiedName);
            injected.Add(typeof(TestSetInjection.Float1).AssemblyQualifiedName);

            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindIntNamedParam<TestSetInjection.Integer1.NamedInt>("5");
            cb.BindNamedParam<TestSetInjection.Float1.NamedFloat, float>("12.5");
            cb.BindList<ListOfClasses, INumber>(injected);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());

            IList<INumber> actual = ((PoolListClass)i.GetInstance(typeof(PoolListClass))).Numbers;

            Assert.True(actual.Count == 3);
            Assert.True(actual.Contains(new TestSetInjection.Integer1(5)));
            Assert.True(actual.Contains(new TestSetInjection.Integer1(5)));
            Assert.True(actual.Contains(new TestSetInjection.Float1(12.5f)));
        }

        [Fact]
        public void TestBindConstructor()
        {
            ICsConfigurationBuilder b = TangFactory.GetTang().NewConfigurationBuilder();
            b.BindConstructor<TestExternalConstructors.A, TestExternalConstructors.ACons>();
            b.BindConstructor<TestExternalConstructors.B, TestExternalConstructors.BCons>();

            TangFactory.GetTang().NewInjector(b.Build()).GetInstance(typeof(TestExternalConstructors.B));
            TangFactory.GetTang().NewInjector(b.Build()).GetInstance(typeof(TestExternalConstructors.A));
        }
    }
}
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
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.Tang
{
    [DefaultImplementation(Name = "Org.Apache.REEF.Tang.Tests.Tang.HaveDefaultStringImplImpl")]
    interface IHaveDefaultStringImpl
    {
    }

    [DefaultImplementation(typeof(HaveDefaultImplImpl))]
    interface IHaveDefaultImpl
    {        
    }

    interface IfaceWithDefault
    {
    }

    [DefaultImplementation(typeof(AnInterfaceImplementation), "default")]
    interface IAnInterface
    {
        void aMethod();
    }

    public class TestDefaultImplementation
    {
        [Fact]
        public void TestDefaultConstructor()
        {
            ClassWithDefaultConstructor impl = (ClassWithDefaultConstructor)TangFactory.GetTang().NewInjector().GetInstance(typeof(ClassWithDefaultConstructor));
            Assert.NotNull(impl);
        }

        [Fact]
        public void TestDefaultImpl()
        {
            ClassWithDefaultConstructor impl = (ClassWithDefaultConstructor)TangFactory.GetTang().NewInjector().GetInstance(typeof(ClassWithDefaultConstructor));
            Assert.NotNull(impl);
        }

        [Fact]
        public void TestDefaultImplOnInterface()
        {
            IAnInterface impl = (IAnInterface)TangFactory.GetTang().NewInjector().GetInstance(typeof(IAnInterface));
            Assert.NotNull(impl);
            impl.aMethod();
        }

        [Fact]
        public void TestGetInstanceOfNamedParameter()
        {
            IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IfaceWithDefault iwd = i.GetNamedInstance<IfaceWithDefaultName, IfaceWithDefault>(GenericType<IfaceWithDefaultName>.Class);
            Assert.NotNull(iwd);
        }

        [Fact]
        public void TestCantGetInstanceOfNamedParameter()
        {
            string msg = null;
            try
            {
                IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
                IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
                i.GetInstance<IfaceWithDefaultName>();
                msg = "getInstance() called on Name IfaceWithDefaultName Did you mean to call getNamedInstance() instead?";
            }
            catch (InjectionException e)
            {
                System.Diagnostics.Debug.WriteLine(e);
            }
            Assert.Null(msg);
        }

        [Fact]
        public void TestCanGetDefaultedInterface()
        {
            Assert.NotNull(TangFactory.GetTang().NewInjector().GetInstance<IHaveDefaultImpl>());
        }

        [Fact]
        public void TestCanOverrideDefaultedInterface()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation(GenericType<IHaveDefaultImpl>.Class, GenericType<OverrideDefaultImpl>.Class);
            var o = TangFactory.GetTang().NewInjector(cb.Build()).GetInstance<IHaveDefaultImpl>();
            Assert.True(o is OverrideDefaultImpl);
        }

        [Fact]
        public void TestCanGetStringDefaultedInterface()
        {
            Assert.NotNull(TangFactory.GetTang().NewInjector().GetInstance<IHaveDefaultStringImpl>());
        }

        [Fact]
        public void TestCanOverrideStringDefaultedInterface()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation(GenericType<IHaveDefaultStringImpl>.Class, GenericType<OverrideDefaultStringImpl>.Class);
            var o = TangFactory.GetTang().NewInjector(cb.Build()).GetInstance<IHaveDefaultStringImpl>();
            Assert.True(o is OverrideDefaultStringImpl);
        }
    }

    public class AnInterfaceImplementation : IAnInterface
    {
        [Inject]
        private AnInterfaceImplementation()
        {
        }

        public void aMethod()
        {
        }
    }

    class IfaceWithDefaultDefaultImpl : IfaceWithDefault
    {
        [Inject]
        IfaceWithDefaultDefaultImpl()
        {            
        }
    }

    [NamedParameter(DefaultClass = typeof(IfaceWithDefaultDefaultImpl))]
    class IfaceWithDefaultName : Name<IfaceWithDefault>
    {
    }

    class HaveDefaultImplImpl : IHaveDefaultImpl
    {
        [Inject]
        HaveDefaultImplImpl()
        {            
        }
    }

    class OverrideDefaultImpl : IHaveDefaultImpl
    {
        [Inject]
        public OverrideDefaultImpl()
        {            
        }
    }

    class HaveDefaultStringImplImpl : IHaveDefaultStringImpl
    {
        [Inject]
        HaveDefaultStringImplImpl()
        {            
        }
    }

    class OverrideDefaultStringImpl : IHaveDefaultStringImpl
    {
        [Inject]
        public OverrideDefaultStringImpl()
        {            
        }
    }

    class ClassWithDefaultConstructor
    {
        [Inject]
        public ClassWithDefaultConstructor()
        {            
        }
    }
}
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

using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Exceptions;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Org.Apache.Reef.Tang.Test.Tang
{
    [DefaultImplementation(Name = "Org.Apache.Reef.Tang.Test.Tang.HaveDefaultStringImplImpl")]
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

    [TestClass]
    public class TestDefaultImplmentation
    {
        [TestMethod]
        public void TestDefaultConstructor()
        {
            ClassWithDefaultConstructor impl = (ClassWithDefaultConstructor)TangFactory.GetTang().NewInjector().GetInstance(typeof(ClassWithDefaultConstructor));
            Assert.IsNotNull(impl);
        }

        [TestMethod]
        public void TestDefaultImplementaion()
        {
            ClassWithDefaultConstructor impl = (ClassWithDefaultConstructor)TangFactory.GetTang().NewInjector().GetInstance(typeof(ClassWithDefaultConstructor));
            Assert.IsNotNull(impl);
        }

        [TestMethod]
        public void TestDefaultImplOnInterface()
        {
            IAnInterface impl = (IAnInterface)TangFactory.GetTang().NewInjector().GetInstance(typeof(IAnInterface));
            Assert.IsNotNull(impl);
            impl.aMethod();
        }

        [TestMethod]
        public void TestGetInstanceOfNamedParameter()
        {
            IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IfaceWithDefault iwd = i.GetNamedInstance<IfaceWithDefaultName, IfaceWithDefault>(GenericType<IfaceWithDefaultName>.Class);
            Assert.IsNotNull(iwd);
        }

        [TestMethod]
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
            Assert.IsNull(msg);
        }

        [TestMethod]
        public void TestCanGetDefaultedInterface()
        {
            Assert.IsNotNull(TangFactory.GetTang().NewInjector().GetInstance<IHaveDefaultImpl>());
        }

        [TestMethod]
        public void TestCanOverrideDefaultedInterface()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation(GenericType<IHaveDefaultImpl>.Class, GenericType<OverrideDefaultImpl>.Class);
            var o = TangFactory.GetTang().NewInjector(cb.Build()).GetInstance<IHaveDefaultImpl>();
            Assert.IsTrue(o is OverrideDefaultImpl);
        }

        [TestMethod]
        public void TestCanGetStringDefaultedInterface()
        {
            Assert.IsNotNull(TangFactory.GetTang().NewInjector().GetInstance<IHaveDefaultStringImpl>());
        }

        [TestMethod]
        public void TestCanOverrideStringDefaultedInterface()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation(GenericType<IHaveDefaultStringImpl>.Class, GenericType<OverrideDefaultStringImpl>.Class);
            var o = TangFactory.GetTang().NewInjector(cb.Build()).GetInstance<IHaveDefaultStringImpl>();
            Assert.IsTrue(o is OverrideDefaultStringImpl);
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
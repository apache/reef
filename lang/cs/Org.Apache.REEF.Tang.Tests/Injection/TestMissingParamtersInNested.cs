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

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Tests.Injection
{
    [TestClass]
    public class TestMissingParamtersInNested
    {
        [TestMethod]
        public void InnerParameterTest()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<ReferencedClass.NamedInt, int>(GenericType<ReferencedClass.NamedInt>.Class, "8");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<ReferencedClass>();
            Assert.IsNotNull(o);
        }

        [TestMethod]
        public void NestedParameterTest()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindNamedParameter<OuterClass.NamedString, string>(GenericType<OuterClass.NamedString>.Class, "foo");
            cb.BindNamedParameter<ReferencedClass.NamedInt, int>(GenericType<ReferencedClass.NamedInt>.Class, "8");
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance<OuterClass>();
            Assert.IsNotNull(o);
        }

        [TestMethod]
        public void MissingAllParameterTest()
        {
            // Cannot inject Org.Apache.REEF.Tang.Tests.Injection.OuterClass, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null: 
            // Org.Apache.REEF.Tang.Tests.Injection.ReferencedClass, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null 
            // missing argument Org.Apache.REEF.Tang.Tests.Injection.ReferencedClass+NamedInt, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
            OuterClass obj = null;
            try
            {
                ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
                IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
                obj = i.GetInstance<OuterClass>();
            }
            catch (InjectionException e)
            {
                System.Diagnostics.Debug.WriteLine(e);
            }
            Assert.IsNull(obj);
        }
    
        [TestMethod]
        public void MissingInnerParameterTest()
        {
            // Cannot inject Org.Apache.REEF.Tang.Tests.Injection.OuterClass, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null: 
            // Org.Apache.REEF.Tang.Tests.Injection.ReferencedClass, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null 
            // missing argument Org.Apache.REEF.Tang.Tests.Injection.ReferencedClass+NamedInt, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
            OuterClass obj = null;
            try
            {
                ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
                cb.BindNamedParameter<OuterClass.NamedString, string>(GenericType<OuterClass.NamedString>.Class, "foo");
                IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
                obj = i.GetInstance<OuterClass>();
            }
            catch (InjectionException e)
            {
                System.Diagnostics.Debug.WriteLine(e);
            }
            Assert.IsNull(obj);
        }

        [TestMethod]
        public void MissingOuterParameterTest()
        {
            // Cannot inject Org.Apache.REEF.Tang.Tests.Injection.OuterClass, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null: 
            // Org.Apache.REEF.Tang.Tests.Injection.OuterClass, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null 
            // missing argument Org.Apache.REEF.Tang.Tests.Injection.OuterClass+NamedString, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
            OuterClass obj = null;
            try
            {
                ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
                cb.BindNamedParameter<ReferencedClass.NamedInt, int>(GenericType<ReferencedClass.NamedInt>.Class, "8");
                IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
                obj = i.GetInstance<OuterClass>();
            }
            catch (InjectionException e)
            {
                System.Diagnostics.Debug.WriteLine(e);
            }
            Assert.IsNull(obj);
        }
    }

    class OuterClass
    {
        [Inject]
        public OuterClass([Parameter(typeof(NamedString))] string s, ReferencedClass refCls)
        {
        }

        [NamedParameter]
        public class NamedString : Name<string>
        {
        }
    }

    class ReferencedClass
    {
        [Inject]
        public ReferencedClass([Parameter(typeof(NamedInt))] int i)
        {           
        }

        [NamedParameter]
        public class NamedInt : Name<int>
        {
        }
    }
}
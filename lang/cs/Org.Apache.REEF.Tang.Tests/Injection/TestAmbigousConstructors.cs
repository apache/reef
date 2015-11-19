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
    public class TestAmbigousConstructors
    {
        [TestMethod]
        public void AmbigousConstructorTest()
        {
            // Cannot inject Org.Apache.REEF.Tang.Tests.Injection.AmbigousConstructorClass, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null 
            // Ambiguous subplan Org.Apache.REEF.Tang.Tests.Injection.AmbigousConstructorClass, Org.Apache.REEF.Tang.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
            // new Org.Apache.REEF.Tang.Tests.Injection.AmbigousConstructorClass(System.String Org.Apache.REEF.Tang.Tests.Injection.AmbigousConstructorClass+NamedString = foo, System.Int32 Org.Apache.REEF.Tang.Tests.Injection.AmbigousConstructorClass+NamedInt = 8) 
            // new Org.Apache.REEF.Tang.Tests.Injection.AmbigousConstructorClass(System.Int32 Org.Apache.REEF.Tang.Tests.Injection.AmbigousConstructorClass+NamedInt = 8, System.String Org.Apache.REEF.Tang.Tests.Injection.AmbigousConstructorClass+NamedString = foo) 
            // ]
            AmbigousConstructorClass obj = null;
            try
            {
                ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
                cb.BindNamedParameter<AmbigousConstructorClass.NamedString, string>(GenericType<AmbigousConstructorClass.NamedString>.Class, "foo");
                cb.BindNamedParameter<AmbigousConstructorClass.NamedInt, int>(GenericType<AmbigousConstructorClass.NamedInt>.Class, "8");
                IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
                obj = i.GetInstance<AmbigousConstructorClass>();
            }
            catch (InjectionException e)
            {
                System.Diagnostics.Debug.WriteLine(e);
            }
            Assert.IsNull(obj);
        }
    }

    class AmbigousConstructorClass
    {
        [Inject]
        public AmbigousConstructorClass([Parameter(typeof(NamedString))] string s, [Parameter(typeof(NamedInt))] int i)
        {
        }

        [Inject]
        public AmbigousConstructorClass([Parameter(typeof(NamedInt))] int i, [Parameter(typeof(NamedString))] string s)
        {
        }

        [NamedParameter]
        public class NamedString : Name<string>
        {
        }

        [NamedParameter]
        public class NamedInt : Name<int>
        {
        }
    }
}
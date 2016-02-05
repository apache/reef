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
using System.Reflection;
using Org.Apache.REEF.Tang.Examples;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.Injection
{
    public class TestForkInjection
    {        
        static Assembly asm = null;

        public TestForkInjection()
        {
            asm = Assembly.Load(FileNames.Examples);
        }

        [Fact]
        public void TestForksInjectorInConstructor()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder(new string[] { FileNames.Examples });
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance(typeof(ForksInjectorInConstructor));
        }

        [Fact]
        public void TestForkWorks()
        {
            Type checkChildIfaceType = typeof(CheckChildIface);
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder(new string[] { FileNames.Examples });
            cb.BindImplementation(GenericType<CheckChildIface>.Class, GenericType<CheckChildImpl>.Class);

            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IInjector i1 = i.ForkInjector();
            CheckChildIface c1 = (CheckChildIface)i1.GetInstance(checkChildIfaceType);
            IInjector i2 = i.ForkInjector();
            CheckChildIface c2 = (CheckChildIface)i2.GetInstance(checkChildIfaceType);
            Assert.NotEqual(c1, c2);
        }
    }
}
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
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.Format
{
    interface IListSuper
    {
    }

    public class TestConfigurationModuleForList
    {
        // ConfigurationModuleBuilder BindList<U, T>(GenericType<U> iface, IParam<IList<T>> opt)
        // public ConfigurationModule Set<T>(IImpl<IList<T>> opt, IList<string> impl)
        [Fact]
        public void ListParamTest()
        {
            IList<string> v = new List<string>();
            v.Add("a");
            v.Add("b");

            IConfiguration c = ListConfigurationModule.CONF
                .Set(ListConfigurationModule.P, v)
                .Build();

            IList<string> s = (IList<string>)TangFactory.GetTang().NewInjector(c).GetNamedInstance(typeof(ListName));
            Assert.Equal(s.Count, 2);
            Assert.True(s.Contains("a"));
            Assert.True(s.Contains("b"));
        }

        // public ConfigurationModuleBuilder BindList<U, T>(GenericType<U> iface, IImpl<IList<T>> opt) where U : Name<IList<T>>
        // public ConfigurationModule Set<T>(IImpl<IList<T>> opt, IList<Type> impl)
        [Fact]
        public void ListImplTest()
        {
            IList<Type> v = new List<Type>();
            v.Add(typeof(ListSubA));
            v.Add(typeof(ListSubB));

            IConfiguration c = ListClassConfigurationModule.CONF
                .Set(ListClassConfigurationModule.P, v)
                .Build();

            IList<IListSuper> s = (IList<IListSuper>)TangFactory.GetTang().NewInjector(c).GetNamedInstance(typeof(ListClass));
            Assert.Equal(s.Count, 2);
            Assert.True(s[0] is ListSubA);
            Assert.True(s[1] is ListSubB);
        }

        // public ConfigurationModuleBuilder BindList<U, T>(GenericType<U> iface, IList<string> impl)
        [Fact]
        public void ListStringTest()
        {
            IConfiguration c = ListIntConfigurationModule.CONF                
                .Build();

            IList<int> i = (IList<int>)TangFactory.GetTang().NewInjector(c).GetNamedInstance(typeof(ListIntName));
            Assert.Equal(i.Count, 2);
            Assert.True(i.Contains(1));
            Assert.True(i.Contains(2));
        }
    }

    [NamedParameter]
    class ListName : Name<IList<string>>
    {
    }

    class ListConfigurationModule : ConfigurationModuleBuilder
    {
        public static readonly RequiredParameter<IList<string>> P = new RequiredParameter<IList<string>>();

        public static readonly ConfigurationModule CONF = new ListConfigurationModule()
            .BindList(GenericType<ListName>.Class, ListConfigurationModule.P)
            .Build();
    }

    [NamedParameter]
    class ListClass : Name<IList<IListSuper>> 
    {
    }

    class ListClassConfigurationModule : ConfigurationModuleBuilder 
    {
        public static readonly RequiredImpl<IList<IListSuper>> P = new RequiredImpl<IList<IListSuper>>();

        public static readonly ConfigurationModule CONF = new ListClassConfigurationModule()
        .BindList(GenericType<ListClass>.Class, ListClassConfigurationModule.P)
        .Build();
    }

    class ListSubA : IListSuper 
    {
        [Inject]
        public ListSubA() 
        {
        }
    }

    class ListSubB : IListSuper 
    {
        [Inject]
        public ListSubB() 
        {
        }
    }

    [NamedParameter]
    class ListIntName : Name<IList<int>>
    {
    }

    class ListIntConfigurationModule : ConfigurationModuleBuilder
    {
        public static readonly ConfigurationModule CONF = new ListIntConfigurationModule()
            .BindList<ListIntName, int>(GenericType<ListIntName>.Class, new List<string>(new string[] { "1", "2" }))
            .Build();
    }
}

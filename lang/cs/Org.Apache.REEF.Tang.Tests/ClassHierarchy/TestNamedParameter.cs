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
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Protobuf;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Tests.ClassHierarchy
{
    [TestClass]
    public class TestNamedParameter
    {
        [TestMethod]
        public void TestNamedParameterWithDefaultValues()
        {
            var ns = TangFactory.GetTang().GetDefaultClassHierarchy();
            INamedParameterNode cls = (INamedParameterNode)ns.GetNode(typeof(NamedParameterWithDefaultValues).AssemblyQualifiedName);
            Assert.IsTrue(cls.GetDocumentation().Equals("NamedParameterWithDefaultValues"));
            Assert.IsTrue(cls.GetShortName().Equals("NamedParameterWithDefaultValues"));
            Assert.IsTrue(cls.GetAlias().Equals("org.apache.REEF.tang.tests.classHierarchy.NamedParameterWithDefaultValues"));
            Assert.IsTrue(cls.GetAliasLanguage().ToString().Equals(Languages.Java.ToString()));
        }

        [TestMethod]
        public void TestNamedParameterWithAlias()
        {
            var ns = TangFactory.GetTang().GetDefaultClassHierarchy();
            INamedParameterNode cls = (INamedParameterNode)ns.GetNode(typeof(NamedParameterWithAlias).AssemblyQualifiedName);
            Assert.IsTrue(cls.GetAlias().Equals("org.apache.REEF.tang.tests.classHierarchy.NamedParameterWithAlias"));
            Assert.IsTrue(cls.GetAliasLanguage().ToString().Equals(Languages.Java.ToString()));
        }

        [TestMethod]
        public void TestNamedParameterWithAliasRoundTrip()
        {
            var ns = TangFactory.GetTang().GetDefaultClassHierarchy();
            INamedParameterNode node1 = (INamedParameterNode)ns.GetNode(typeof(NamedParameterWithAlias).AssemblyQualifiedName);

            var ns1 = new ProtocolBufferClassHierarchy(ProtocolBufferClassHierarchy.Serialize(ns));
            var node2 = ns1.GetNode(typeof(NamedParameterWithAlias).AssemblyQualifiedName);

            Assert.IsTrue(node2 is INamedParameterNode);
            Assert.IsTrue(((INamedParameterNode)node2).GetAliasLanguage().ToString().Equals(Languages.Java.ToString()));
            Assert.IsTrue(((INamedParameterNode)node2).GetFullName().Equals(typeof(NamedParameterWithAlias).AssemblyQualifiedName));
            Assert.IsTrue(((INamedParameterNode)node2).GetAlias().Equals("org.apache.REEF.tang.tests.classHierarchy.NamedParameterWithAlias"));
        }

        [TestMethod]
        public void TestGetNamedparameterValue()
        {
            var b = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<NamedParameterWithAlias, string>(GenericType<NamedParameterWithAlias>.Class, "test")
                .Build();

            var c = b.GetClassHierarchy();
            var i = TangFactory.GetTang().NewInjector(b);
            var o = i.GetInstance<ClassWithNamedParameterWithAlias>();
            var no = i.GetNamedInstance<NamedParameterWithAlias, string>();
            Assert.IsTrue(o.Value.Equals("test"));
        }      
    }

    [NamedParameter(Documentation = "NamedParameterWithDefaultValues",
        ShortName = "NamedParameterWithDefaultValues",
        DefaultValue = "default",
        DefaultClass = null,
        DefaultValues = null,
        DefaultClasses = null,
        Alias = "org.apache.REEF.tang.tests.classHierarchy.NamedParameterWithDefaultValues",
        AliasLanguage = Languages.Java
     )]

    public class NamedParameterWithDefaultValues : Name<string> 
    {
    }

    [NamedParameter(alias: "org.apache.REEF.tang.tests.classHierarchy.NamedParameterWithAlias", aliasLanguage: Languages.Java)]
    public class NamedParameterWithAlias : Name<string>
    {
    }

    public class ClassWithNamedParameterWithAlias
    {
        public string Value;

        [Inject]
        private ClassWithNamedParameterWithAlias([Parameter(typeof(NamedParameterWithAlias))] string abc)
        {
            Value = abc;
        }
    }
}

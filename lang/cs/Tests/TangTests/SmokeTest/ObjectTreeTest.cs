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

using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Org.Apache.Reef.Tang.Test.SmokeTest
{
    [TestClass]
    public class ObjectTreeTest
    {
        public static IConfiguration GetConfiguration()
        {
            return TestConfigurationModuleBuilder.CONF
                .Set(TestConfigurationModuleBuilder.OPTIONALSTRING, TestConfigurationModuleBuilder.OptionaStringValue)
                .Set(TestConfigurationModuleBuilder.REQUIREDSTRING, TestConfigurationModuleBuilder.RequiredStringValue)
                .Build();
        }

        [TestMethod]
        public void TestInstantiation() 
        {
            IRootInterface root = TangFactory.GetTang().NewInjector(GetConfiguration()).GetInstance<IRootInterface>();
            Assert.IsTrue(root.IsValid(), "Object instantiation left us in an inconsistent state.");
        }

        [TestMethod]
        public void TestTwoInstantiations() 
        {
            IRootInterface firstRoot = TangFactory.GetTang().NewInjector(GetConfiguration()).GetInstance<IRootInterface>();
            IRootInterface secondRoot = TangFactory.GetTang().NewInjector(GetConfiguration()).GetInstance<IRootInterface>();

            Assert.AreNotSame(firstRoot, secondRoot, "Two instantiations of the object tree should not be the same");
            Assert.AreEqual(firstRoot, secondRoot, "Two instantiations of the object tree should be equal");
        }
    }
}

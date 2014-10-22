/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
﻿using System.Collections.Generic;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Types;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Com.Microsoft.TangTest.ClassHierarchy
{
    [TestClass]
    public class TestClassHierarchy
    {
        public static IClassHierarchy ns;
        static string file = @"Com.Microsoft.Tang.Examples";
        static string file2 = @"com.microsoft.reef.activity";
        static string file3 = @"com.microsoft.reef.activityInterface";

        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            ns = TangFactory.GetTang().GetClassHierarchy(new string[] {file, file2, file3});
            //ns = new ClassHierarchyImpl(@"Com.Microsoft.Tang.Examples.dll");
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
        }

        [TestInitialize()]
        public void TestSetup()
        {
        }

        [TestCleanup()]
        public void TestCleanup()
        {
        }

        [TestMethod]
        public void TestString()
        {
            INode n = null;
            try
            {
                n = ns.GetNode("System");
            }
            catch (NameResolutionException e)
            {
            }
            Assert.IsNull(n);

            Assert.IsNotNull(ns.GetNode("System.String"));
            try
            {
                ns.GetNode("com.microsoft");
                Assert.Fail("Didn't get expected exception");
            }
            catch (NameResolutionException e)
            {

            }
        }

        [TestMethod]
        public void TestInt()
        {
            INode n = null;
            try
            {
                n = ns.GetNode("System");
            }
            catch (NameResolutionException e)
            {
            }
            Assert.IsNull(n);

            Assert.IsNotNull(ns.GetNode("System.Int32"));
            try
            {
                ns.GetNode("com.microsoft");
                Assert.Fail("Didn't get expected exception");
            }
            catch (NameResolutionException e)
            {

            }
        }
      
        [TestMethod]
        public void TestSimpleConstructors() 
        {
            IClassNode cls = (IClassNode)ns.GetNode("Com.Microsoft.Tang.Examples.SimpleConstructors");
            Assert.IsTrue(cls.GetChildren().Count == 0);
            IList<IConstructorDef> def = cls.GetInjectableConstructors();
            Assert.AreEqual(3, def.Count);
        }

        [TestMethod]
        public void TestTimer() 
        {
            IClassNode timerClassNode = (IClassNode)ns.GetNode("Com.Microsoft.Tang.Examples.Timer");
            INode secondNode = ns.GetNode("Com.Microsoft.Tang.Examples.Timer+Seconds");
            Assert.AreEqual(secondNode.GetFullName(), "Com.Microsoft.Tang.Examples.Timer+Seconds");

        }

        [TestMethod]
        public void TestNamedParameterConstructors()
        {
            var node = ns.GetNode("Com.Microsoft.Tang.Examples.NamedParameterConstructors");
            Assert.AreEqual(node.GetFullName(), "Com.Microsoft.Tang.Examples.NamedParameterConstructors");
        }

        [TestMethod]
        public void TestNamedParameterIdentifier()
        {
            var node = ns.GetNode("com.microsoft.reef.driver.activity.ActivityConfigurationOptions+Identifier");
            Assert.AreEqual(node.GetFullName(), "com.microsoft.reef.driver.activity.ActivityConfigurationOptions+Identifier");
        }


        [TestMethod]
        public void TestActivityNode()
        {
            var node = ns.GetNode("com.microsoft.reef.activity.HelloActivity");
            Assert.AreEqual(node.GetFullName(), "com.microsoft.reef.activity.HelloActivity");
        }

        [TestMethod]
        public void TestIActivityNode()
        {
            var node = ns.GetNode("com.microsoft.reef.activity.IActivity");
            Assert.AreEqual(node.GetFullName(), "com.microsoft.reef.activity.IActivity");
        }


        [TestMethod]
        public void TestDocumentedLocalNamedParameter() 
        {
            var node = ns.GetNode("Com.Microsoft.Tang.Examples.DocumentedLocalNamedParameter");
            Assert.AreEqual(node.GetFullName(), "Com.Microsoft.Tang.Examples.DocumentedLocalNamedParameter");
        }

        [TestMethod]
        public void TestOKShortNames()
        {
            var node = ns.GetNode("Com.Microsoft.Tang.Examples.ShortNameFooA");
            Assert.AreEqual(node.GetFullName(), "Com.Microsoft.Tang.Examples.ShortNameFooA");
        }

        //[TestMethod]
        //exception would throw when building the hierarchy
        public void testConflictingShortNames()
        {
            var nodeA = ns.GetNode("Com.Microsoft.Tang.Examples.ShortNameFooA");
            var nodeB = ns.GetNode("Com.Microsoft.Tang.Examples.ShortNameFooB");
        }

        [TestMethod]
        public void testResolveDependencies() 
        {
            ns.GetNode("Com.Microsoft.Tang.Examples.SimpleConstructors");
            Assert.IsNotNull(ns.GetNode("System.String"));
        }
    }
}

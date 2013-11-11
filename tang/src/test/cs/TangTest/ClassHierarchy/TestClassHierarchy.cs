using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            ns = new ClassHierarchyImpl(@"Com.Microsoft.Tang.Examples.dll");
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            System.Console.WriteLine("ClassCleanup");
        }

        [TestInitialize()]
        public void TestSetup()
        {
            System.Console.WriteLine("TestSetup");
        }

        [TestCleanup()]
        public void TestCleanup()
        {
            System.Console.WriteLine("TestCleanup");
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

        }

        [TestMethod]
        public void TestNamedParameterConstructors()
        {
            var node = ns.GetNode("Com.Microsoft.Tang.Examples.NamedParameterConstructors");

        }

        [TestMethod]
        public void TestDocumentedLocalNamedParameter() 
        {
            ns.GetNode("Com.Microsoft.Tang.Examples.DocumentedLocalNamedParameter");
        }

        [TestMethod]
        public void TestOKShortNames()
        {
            ns.GetNode("Com.Microsoft.Tang.Examples.ShortNameFooA");
        }

        //[TestMethod]
        //exception would throw when building the hierarchy
        public void testConflictingShortNames()
        {
            ns.GetNode("Com.Microsoft.Tang.Examples.ShortNameFooA");
            ns.GetNode("Com.Microsoft.Tang.Examples.ShortNameFooB");
        }

        [TestMethod]
        public void testResolveDependencies() 
        {
            ns.GetNode("Com.Microsoft.Tang.Examples.SimpleConstructors");
            Assert.IsNotNull(ns.GetNode("System.String"));
        }
    }
}

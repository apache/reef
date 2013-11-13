using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Protobuf;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Com.Microsoft.TangTest.ClassHierarchy
{
    [TestClass]
    public class TestSerilization
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
        public void TestSerializeClassHierarchy()
        {
            ProtocolBufferClassHierarchy.Serialize("node.bin", ns);
        }

        [TestMethod]
        public void TestDeSerializeClassHierarchy()
        {
            ProtocolBufferClassHierarchy.DeSerialize("node.bin");
        }
    }
}

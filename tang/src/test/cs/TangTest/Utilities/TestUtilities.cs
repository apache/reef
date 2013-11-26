using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Com.Microsoft.TangTest.Utilities
{
    [TestClass]
    public class TestUtilities
    {
        [TestMethod]
        public void TestGetEnclosingClassFullNameByName()
        {
            string[] path = ClassNameParser.GetEnclosingClassFullNames("Com.Microsoft.Tang.Examples.A+B+C");
            Assert.AreEqual(path[0], "Com.Microsoft.Tang.Examples.A");
            Assert.AreEqual(path[1], "Com.Microsoft.Tang.Examples.A+B");
            Assert.AreEqual(path[2], "Com.Microsoft.Tang.Examples.A+B+C");
        }

        [TestMethod]
        public void TestGetEnclosingClassFullNameByType()
        {
            var asm = Assembly.Load(@"Com.Microsoft.Tang.Examples");
            Type seconds = asm.GetType(@"Com.Microsoft.Tang.Examples.Timer+Seconds");
            Type B2 = asm.GetType(@"Com.Microsoft.Tang.Examples.B+B1+B2");
            Type timer = typeof(Com.Microsoft.Tang.Examples.Timer);

            string[] pathSeconds = ClassNameParser.GetEnclosingClassFullNames(seconds);
            Assert.AreEqual(pathSeconds[0], "Com.Microsoft.Tang.Examples.Timer");
            Assert.AreEqual(pathSeconds[1], "Com.Microsoft.Tang.Examples.Timer+Seconds");

            string[] pathB2 = ClassNameParser.GetEnclosingClassFullNames(B2);
            Assert.AreEqual(pathB2[0], "Com.Microsoft.Tang.Examples.B");
            Assert.AreEqual(pathB2[1], "Com.Microsoft.Tang.Examples.B+B1");
            Assert.AreEqual(pathB2[2], "Com.Microsoft.Tang.Examples.B+B1+B2");

            string[] pathTime = ClassNameParser.GetEnclosingClassFullNames(timer);
            Assert.AreEqual(pathTime[0], "Com.Microsoft.Tang.Examples.Timer");
        }

        [TestMethod]
        public void TestGetEnclosingClassShortNameByType()
        {
            var asm = Assembly.Load(@"Com.Microsoft.Tang.Examples");
            Type seconds = asm.GetType(@"Com.Microsoft.Tang.Examples.Timer+Seconds");
            Type B2 = asm.GetType(@"Com.Microsoft.Tang.Examples.B+B1+B2");
            Type timer = typeof(Com.Microsoft.Tang.Examples.Timer);

            string[] pathSeconds = ClassNameParser.GetEnclosingClassShortNames(seconds);
            Assert.AreEqual(pathSeconds[0], "Timer");
            Assert.AreEqual(pathSeconds[1], "Seconds");

            string[] pathB2 = ClassNameParser.GetEnclosingClassShortNames(B2);
            Assert.AreEqual(pathB2[0], "B");
            Assert.AreEqual(pathB2[1], "B1");
            Assert.AreEqual(pathB2[2], "B2");

            string[] pathTime = ClassNameParser.GetEnclosingClassShortNames(timer);
            Assert.AreEqual(pathTime[0], "Timer");
        }

        [TestMethod]
        public void TestGetEnclosingClassShortNameByName()
        {
            string[] path = ClassNameParser.GetEnclosingClassShortNames("Com.Microsoft.Tang.Examples.A+B+C");
            Assert.AreEqual(path[0], "A");
            Assert.AreEqual(path[1], "B");
            Assert.AreEqual(path[2], "C");
        }
    }


}

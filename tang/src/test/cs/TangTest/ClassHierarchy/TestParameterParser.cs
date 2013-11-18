using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Implementations;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Com.Microsoft.TangTest.ClassHierarchy
{
    [TestClass]
    public class TestParameterParser
    {
        [TestMethod]
        public void ParseIntTest()
        {
            var parser = new ParameterParser();
            Int32 o = (Int32)parser.Parse(typeof(Int32), "4");

        }

        [TestMethod]
        public void ParseBoolTest()
        {
            var parser = new ParameterParser();
            Boolean o = (Boolean)parser.Parse(typeof(Boolean), "false");

        }
    }
}

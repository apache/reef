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

        [TestMethod]
        public void ParseLongTest()
        {
            var parser = new ParameterParser();
            long o = (long)parser.Parse(typeof(long), "8675309");

        }

        [TestMethod]
        public void ParseStringTest()
        {
            var parser = new ParameterParser();
            string o = (string)parser.Parse(typeof(string), "hello");

        }

        [TestMethod]
        public void ParseDoubleTest()
        {
            var parser = new ParameterParser();
            Double o = (Double)parser.Parse(typeof(double), "12.6");

        }

        [TestMethod]
        public void ParseCharTest()
        {
            var parser = new ParameterParser();
            Char o = (Char)parser.Parse(typeof(char), "c");

        }

        [TestMethod]
        public void ParseByteTest()
        {
            var parser = new ParameterParser();
            Byte o = (Byte)parser.Parse(typeof(byte), "8");

        }

        [TestMethod]
        public void ParseShortTest()
        {
            var parser = new ParameterParser();
            Int16 o = (Int16)parser.Parse(typeof(short), "8");
        }

        [TestMethod]
        public void ParseFloatTest()
        {
            var parser = new ParameterParser();
            Single o = (Single)parser.Parse(typeof(float), "8.567");
        }

        [TestMethod]
        public void ParseByteArrayTest()
        {
            var parser = new ParameterParser();
            byte[] o = (byte[])parser.Parse(typeof(byte[]), "hello");

        }
    }
}

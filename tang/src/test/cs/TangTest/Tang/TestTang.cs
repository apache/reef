using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Com.Microsoft.Tang.Examples;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Interface;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Com.Microsoft.TangTest.Tang
{
    [TestClass]
    public class TestTang
    {
        static string file = @"Com.Microsoft.Tang.Examples";

        static Assembly asm = null;

        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            asm = Assembly.Load(file);
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

        //[TestMethod]
        public void TestForksInjectorInConstructor()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder(new string[] { file });
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            var o = i.GetInstance(typeof(ForksInjectorInConstructor));
        }

        [TestMethod]
        public void TestForkWorks() 
        {
            Type checkChildIfaceType = typeof(Com.Microsoft.Tang.Examples.CheckChildIface);
            Type checkChildImplType = typeof(Com.Microsoft.Tang.Examples.CheckChildImpl);
            
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder(new string[] { file });
            cb.Bind(checkChildIfaceType, checkChildImplType);
    
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IInjector i1 = i.ForkInjector();
            CheckChildIface c1 = (CheckChildIface)i1.GetInstance(checkChildIfaceType);
            IInjector i2 = i.ForkInjector();
            CheckChildIface c2 = (CheckChildIface)i2.GetInstance(checkChildIfaceType);
            Assert.AreNotEqual(c1, c2);
        }
    }
}

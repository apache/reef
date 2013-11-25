using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.formats;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Protobuf;
using Com.Microsoft.Tang.Types;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Com.Microsoft.TangTest.ClassHierarchy
{
    [TestClass]
    public class TestSerilization
    {
        public static string file = @"Com.Microsoft.Tang.Examples.dll";
        static Assembly asm = null;

        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            asm = Assembly.LoadFrom(file);
            Assembly.LoadFrom(@"com.microsoft.reef.activity.dll");
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
        public void TestSerializeClassHierarchy()
        {
            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(@"Com.Microsoft.Tang.Examples.dll");
            IClassNode timerClassNode = (IClassNode)ns.GetNode("Com.Microsoft.Tang.Examples.Timer");
            ProtocolBufferClassHierarchy.Serialize("node.bin", ns);
        }

        [TestMethod]
        public void TestDeSerializeClassHierarchy()
        {
            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(@"Com.Microsoft.Tang.Examples.dll");
            IClassNode timerClassNode = (IClassNode)ns.GetNode("Com.Microsoft.Tang.Examples.Timer");
            INode secondNode = (INode)ns.GetNode("Com.Microsoft.Tang.Examples.Timer+Seconds");
            IClassNode SimpleConstructorsClassNode = (IClassNode)ns.GetNode("Com.Microsoft.Tang.Examples.SimpleConstructors");
            
            ProtocolBufferClassHierarchy.Serialize("node.bin", ns);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize("node.bin");

            IClassNode timerClassNode2 = (IClassNode)ch.GetNode("Com.Microsoft.Tang.Examples.Timer");
            INode secondNode2 = ch.GetNode("Com.Microsoft.Tang.Examples.Timer+Seconds");
            IClassNode SimpleConstructorsClassNode2 = (IClassNode)ch.GetNode("Com.Microsoft.Tang.Examples.SimpleConstructors");

            Assert.AreEqual(timerClassNode.GetFullName(), timerClassNode2.GetFullName());
            Assert.AreEqual(secondNode.GetFullName(), secondNode.GetFullName());
            Assert.AreEqual(SimpleConstructorsClassNode.GetFullName(), SimpleConstructorsClassNode2.GetFullName());

            Assert.IsTrue(SimpleConstructorsClassNode2.GetChildren().Count == 0);
            IList<IConstructorDef> def = SimpleConstructorsClassNode2.GetInjectableConstructors();
            Assert.AreEqual(3, def.Count);
        }

        [TestMethod]
        public void TestDeSerializeClassHierarchyForActivity()
        {
            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(@"com.microsoft.reef.activity.dll");
            IClassNode activityClassNode = (IClassNode)ns.GetNode("com.microsoft.reef.activity.HelloActivity");
   
            ProtocolBufferClassHierarchy.Serialize("activity.bin", ns);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize("activity.bin");

            IClassNode activityClassNode2 = (IClassNode)ch.GetNode("com.microsoft.reef.activity.HelloActivity");

            Assert.AreEqual(activityClassNode.GetFullName(), activityClassNode2.GetFullName());
        }

        [TestMethod]
        public void TestSerirializeInjectionPlanForTimer()
        {
            Type timerType = typeof(Com.Microsoft.Tang.Examples.Timer);
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.Timer+Seconds");

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            cb.BindNamedParameter(namedParameter, "2");
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            InjectionPlan ip = injector.GetInjectionPlan(timerType);

            ProtocolBufferInjectionPlan.Serialize("timerplan.bin", ip);
        }

        [TestMethod]
        public void TestDeSerirializeInjectionPlanForTimer()
        {
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.Timer+Seconds");
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            cb.BindNamedParameter(namedParameter, "2");
            IConfiguration conf = cb.Build();

            var ch = conf.GetClassHierarchy();
            var ip = ProtocolBufferInjectionPlan.DeSerialize("timerplan.bin", ch);
        }

        [TestMethod]
        public void TestSerirializeInjectionPlanForSimpleConstructor()
        {
            Type simpleConstructorType = typeof(Com.Microsoft.Tang.Examples.SimpleConstructors);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            InjectionPlan ip = injector.GetInjectionPlan(simpleConstructorType);

            ProtocolBufferInjectionPlan.Serialize("plan.bin", ip);
        }

        [TestMethod]
        public void TestDeSerirializeInjectionPlanForSimpleConstructor()
        {
            //Type simpleConstructorType = typeof(Com.Microsoft.Tang.Examples.SimpleConstructors);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            IConfiguration conf = cb.Build();

            var ch = conf.GetClassHierarchy();
            var ip = ProtocolBufferInjectionPlan.DeSerialize("plan.bin", ch);
        }

    }
}

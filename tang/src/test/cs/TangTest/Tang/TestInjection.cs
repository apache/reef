using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Examples;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Interface;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Com.Microsoft.TangTest.Tang
{
    [TestClass]
    public class TestInjection
    {
        static string file = @"Com.Microsoft.Tang.Examples.dll";
        static Assembly asm = null;

        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            asm = Assembly.LoadFrom(file);
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
        public void TestTimer()
        {
            Type timerType = typeof(Com.Microsoft.Tang.Examples.Timer);
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.Timer+Seconds");

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            cb.BindNamedParameter(namedParameter, "2");
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var timer = (Com.Microsoft.Tang.Examples.Timer)injector.GetInstance(timerType);

            Assert.IsNotNull(timer);

            System.Console.WriteLine("Tick...");
            timer.sleep();
            System.Console.WriteLine("Tock...");
        }

        [TestMethod]
        public void TestTimerWithClassHierarchy()
        {
            Type timerType = typeof(Com.Microsoft.Tang.Examples.Timer);
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.Timer+Seconds");

            ClassHierarchyImpl classHierarchyImpl = new ClassHierarchyImpl(file);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(classHierarchyImpl);
            cb.BindNamedParameter(namedParameter, "2");
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var timer = (Com.Microsoft.Tang.Examples.Timer)injector.GetInstance(timerType);

            Assert.IsNotNull(timer);

            System.Console.WriteLine("Tick...");
            timer.sleep();
            System.Console.WriteLine("Tock...");
        }

        [TestMethod]
        public void TestDocumentLoadNamedParameter()
        {
            Type documentedLocalNamedParameterType = typeof(Com.Microsoft.Tang.Examples.DocumentedLocalNamedParameter);
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.DocumentedLocalNamedParameter+Foo");

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            cb.BindNamedParameter(namedParameter, "Hello");
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var doc = (Com.Microsoft.Tang.Examples.DocumentedLocalNamedParameter)injector.GetInstance(documentedLocalNamedParameterType);

            Assert.IsNotNull(doc);
            var s = doc.ToString();
        }

        [TestMethod]
        public void TestSimpleConstructor()
        {
            Type simpleConstructorType = typeof(Com.Microsoft.Tang.Examples.SimpleConstructors);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var simpleConstructor = (Com.Microsoft.Tang.Examples.SimpleConstructors)injector.GetInstance(simpleConstructorType);
            Assert.IsNotNull(simpleConstructor);
        }

        [TestMethod]
        public void TestActivity()
        {
            Type activityType = typeof(Com.Microsoft.Tang.Examples.HelloActivity);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var activityRef = (Com.Microsoft.Tang.Examples.HelloActivity)injector.GetInstance(activityType);
            Assert.IsNotNull(activityRef);

            byte[] b = new byte[10];
            activityRef.Call(b);
        }

        [TestMethod]
        public void TestTweetExample()
        {
            Type tweeterType = typeof(Com.Microsoft.Tang.Examples.Tweeter);
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.Tweeter+PhoneNumber");
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });

            cb.BindImplementation(typeof(TweetFactory), typeof(MockTweetFactory));
            cb.BindImplementation(typeof(SMS), typeof(MockSMS));
            cb.BindNamedParameter(namedParameter, "8675309");
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var tweeter = (Com.Microsoft.Tang.Examples.Tweeter)injector.GetInstance(tweeterType);
            tweeter.sendMessage();
        }
    }
}

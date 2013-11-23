using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Examples;
using Com.Microsoft.Tang.formats;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Protobuf;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Com.Microsoft.TangTest.Tang
{
    [TestClass]
    public class TestConfiguration
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
        public void TestActivityConfiguration()
        {
            Type activityInterfaceType = typeof(Com.Microsoft.Tang.Examples.IActivity);
            Type activityType = typeof(Com.Microsoft.Tang.Examples.HelloActivity);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            cb.BindImplementation(activityInterfaceType, activityType);
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "activityConf.txt");
            IDictionary<string, string> p = ReadFromFile("activityConf.txt");


            ITang tang1 = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang1.NewConfigurationBuilder(new string[] { file });
            ConfigurationFile.AddConfiguration(cb1, "activityConf.txt");
            IConfiguration conf1 = cb1.Build();

            IInjector injector = tang1.NewInjector(conf1);
            var activityRef = (Com.Microsoft.Tang.Examples.HelloActivity)injector.GetInstance(activityInterfaceType);
            Assert.IsNotNull(activityRef);

            byte[] b = new byte[10];
            activityRef.Call(b);
        }

        [TestMethod]
        public void TestActivityConfigWithSeperateAssembly()
        {
            Type activityInterfaceType = typeof(com.microsoft.reef.activity.IActivity);
            Type activityType = typeof(com.microsoft.reef.activity.HelloActivity);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { @"com.microsoft.reef.activity.dll" });
            cb.BindImplementation(activityInterfaceType, activityType);
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "activityConf1.txt");
            IDictionary<string, string> p = ReadFromFile("activityConf1.txt");

            IInjector injector = tang.NewInjector(new string[] { @"com.microsoft.reef.activity.dll" }, "activityConf1.txt");
            var activityRef = (com.microsoft.reef.activity.IActivity)injector.GetInstance(activityInterfaceType);

            //combined line sample
            var o = (com.microsoft.reef.activity.HelloActivity)TangFactory.GetTang()
                .NewInjector(new string[] { @"com.microsoft.reef.activity.dll" }, "activityConf1.txt")
                .GetInstance(typeof(com.microsoft.reef.activity.IActivity));

            Assert.IsNotNull(activityRef);
            byte[] b = new byte[10];
            activityRef.Call(b);
        }

        [TestMethod]
        public void TestGetConfgiFromProtoBufClassHierarchy()
        {
            Type activityInterfaceType = typeof(com.microsoft.reef.activity.IActivity);
            Type activityType = typeof(com.microsoft.reef.activity.HelloActivity);

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(@"com.microsoft.reef.activity.dll");
            ProtocolBufferClassHierarchy.Serialize("activity.bin", ns);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize("activity.bin");
            ITang tang = TangFactory.GetTang();
            IConfigurationBuilder cb = tang.NewConfigurationBuilder(ch);
            cb.Bind("com.microsoft.reef.activity.IActivity", "com.microsoft.reef.activity.HelloActivity");

            IConfiguration conf = cb.Build();
            ConfigurationFile.WriteConfigurationFile(conf, "activityConf2.txt");
        }

        [TestMethod]
        public void TestActivityConfig()
        {
            Type activityInterfaceType = typeof(Com.Microsoft.Tang.Examples.IActivity);
            Type activityType = typeof(Com.Microsoft.Tang.Examples.HelloActivity);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            cb.BindImplementation(activityInterfaceType, activityType);
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "activityConf.txt");
            IDictionary<string, string> p = ReadFromFile("activityConf.txt");

            IInjector injector = tang.NewInjector(new string[] { file }, "activityConf.txt");
            var activityRef = (Com.Microsoft.Tang.Examples.HelloActivity)injector.GetInstance(activityInterfaceType);

            Assert.IsNotNull(activityRef);
            byte[] b = new byte[10];
            activityRef.Call(b);
        }

        [TestMethod]
        public void TestTweetConfiguration()
        {
            Type tweeterType = typeof(Com.Microsoft.Tang.Examples.Tweeter);
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.Tweeter+PhoneNumber");
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });

            cb.BindImplementation(typeof(TweetFactory), typeof(MockTweetFactory));
            cb.BindImplementation(typeof(SMS), typeof(MockSMS));
            cb.BindNamedParameter(namedParameter, "8675309");
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "tweeterConf.txt");
            IDictionary<string, string> p = ReadFromFile("tweeterConf.txt");

            ITang tang1 = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang1.NewConfigurationBuilder(new string[] { file });
            ConfigurationFile.AddConfiguration(cb1, "tweeterConf.txt");
            IConfiguration conf1 = cb1.Build();

            IInjector injector = tang1.NewInjector(conf1);
            var tweeter = (Com.Microsoft.Tang.Examples.Tweeter)injector.GetInstance(tweeterType);
            tweeter.sendMessage();
        }

        [TestMethod]
        public void TestTweetConfig()
        {
            Type tweeterType = typeof(Com.Microsoft.Tang.Examples.Tweeter);
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.Tweeter+PhoneNumber");
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });

            cb.BindImplementation(typeof(TweetFactory), typeof(MockTweetFactory));
            cb.BindImplementation(typeof(SMS), typeof(MockSMS));
            cb.BindNamedParameter(namedParameter, "8675309");
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "tweeterConf.txt");
            IDictionary<string, string> p = ReadFromFile("tweeterConf.txt");

            IInjector injector = tang.NewInjector(new string[] { file }, "tweeterConf.txt");
            var tweeter = (Com.Microsoft.Tang.Examples.Tweeter)injector.GetInstance(tweeterType);
            tweeter.sendMessage();
        }

        [TestMethod]
        public void TestTimerConfiguration()
        {
            Type timerType = typeof(Com.Microsoft.Tang.Examples.Timer);
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.Timer+Seconds");

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            cb.BindNamedParameter(namedParameter, "2");
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "timerConf.txt");
            IDictionary<string, string> p = ReadFromFile("timerConf.txt");

            ITang tang1 = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang1.NewConfigurationBuilder(new string[] { file });
            ConfigurationFile.AddConfiguration(cb1, "timerConf.txt");
            IConfiguration conf1 = cb1.Build();

            IInjector injector = tang.NewInjector(conf1);
            var timer = (Com.Microsoft.Tang.Examples.Timer)injector.GetInstance(timerType);

            Assert.IsNotNull(timer);

            System.Console.WriteLine("Tick...");
            timer.sleep();
            System.Console.WriteLine("Tock...");
        }

        [TestMethod]
        public void TestDocumentLoadNamedParameterConfiguration()
        {
            Type documentedLocalNamedParameterType = typeof(Com.Microsoft.Tang.Examples.DocumentedLocalNamedParameter);
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.DocumentedLocalNamedParameter+Foo");

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file });
            cb.BindNamedParameter(namedParameter, "Hello");
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "docLoadConf.txt");
            IDictionary<string, string> p = ReadFromFile("docLoadConf.txt");

            ITang tang1 = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang1.NewConfigurationBuilder(new string[] { file });
            ConfigurationFile.AddConfiguration(cb1, "docLoadConf.txt");
            IConfiguration conf1 = cb1.Build();

            IInjector injector = tang1.NewInjector(conf1);
            var doc = (Com.Microsoft.Tang.Examples.DocumentedLocalNamedParameter)injector.GetInstance(documentedLocalNamedParameterType);

            Assert.IsNotNull(doc);
            var s = doc.ToString();
        }



        [TestMethod]
        public void TestTimerConfigurationWithClassHierarchy()
        {
            Type timerType = typeof(Com.Microsoft.Tang.Examples.Timer);
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.Timer+Seconds");

            ClassHierarchyImpl classHierarchyImpl = new ClassHierarchyImpl(file);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(classHierarchyImpl);
            cb.BindNamedParameter(namedParameter, "1");
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "timerConfH.txt");
            IDictionary<string, string> p = ReadFromFile("timerConfH.txt");

            ITang tang1 = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang1.NewConfigurationBuilder(new string[] { file });
            ConfigurationFile.AddConfiguration(cb1, "timerConfH.txt");
            IConfiguration conf1 = cb1.Build();

            IInjector injector = tang1.NewInjector(conf1);
            var timer = (Com.Microsoft.Tang.Examples.Timer)injector.GetInstance(timerType);

            Assert.IsNotNull(timer);
            timer.sleep();
        }

        private static IDictionary<string, string> ReadFromFile(string fileName)
        {
            IDictionary<string, string> property = new Dictionary<string, string>();
            using (StreamReader sr = new StreamReader(fileName))
            {
                while (!sr.EndOfStream)
                {
                    string line = sr.ReadLine();
                    string[] p = line.Split('=');
                    property.Add(p[0], p[1]);
                }
            }
            return property;
        }
    }
}

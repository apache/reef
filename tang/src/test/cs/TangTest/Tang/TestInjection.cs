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
﻿using System;
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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Com.Microsoft.TangTest.Tang
{
    [TestClass]
    public class TestInjection
    {
        static string file = @"Com.Microsoft.Tang.Examples";
        static string file2 = @"com.microsoft.reef.activity";
        static string file3 = @"com.microsoft.reef.activityInterface";

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
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder((ICsClassHierarchy)classHierarchyImpl);

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
            var a = Assembly.Load(@"com.microsoft.reef.activity");
            Type activityType = a.GetType("com.microsoft.reef.activity.HelloActivity");

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { @"com.microsoft.reef.activity", @"com.microsoft.reef.ActivityInterface" });
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var activityRef = (com.microsoft.reef.activity.IActivity)injector.GetInstance(activityType);
            Assert.IsNotNull(activityRef);

            //byte[] b = new byte[10];
            //activityRef.Call(b);
        }

        [TestMethod]
        public void TestStreamActivity1()
        {
            var a = Assembly.Load(@"com.microsoft.reef.activity");
            Type activityType = a.GetType("com.microsoft.reef.activity.StreamActivity1");

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { @"com.microsoft.reef.activity", @"com.microsoft.reef.ActivityInterface" });
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var activityRef = (com.microsoft.reef.activity.IActivity)injector.GetInstance(activityType);
            Assert.IsNotNull(activityRef);

            //activityRef.Call(null);
        }

        [TestMethod]
        public void TestStreamActivity2()
        {
            var a = Assembly.Load(@"com.microsoft.reef.activity");
            Type activityType = a.GetType("com.microsoft.reef.activity.StreamActivity2");

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { @"com.microsoft.reef.activity", @"com.microsoft.reef.ActivityInterface" });
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var activityRef = (com.microsoft.reef.activity.IActivity)injector.GetInstance(activityType);
            Assert.IsNotNull(activityRef);

            //activityRef.Call(null);
        }

        [TestMethod]
        public void TestMultipleAssemlies()
        {
            Type activityInterfaceType1 = typeof(com.microsoft.reef.activity.IActivity);
            var a = Assembly.Load(@"com.microsoft.reef.activity");
            Type activityType1 = a.GetType("com.microsoft.reef.activity.HelloActivity");

            Type tweeterType = typeof(Com.Microsoft.Tang.Examples.Tweeter);
            Type namedParameter = asm.GetType(@"Com.Microsoft.Tang.Examples.Tweeter+PhoneNumber");

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file, file2, file3 });
            cb.BindImplementation(activityInterfaceType1, activityType1);
            cb.BindImplementation(typeof(TweetFactory), typeof(MockTweetFactory));
            cb.BindImplementation(typeof(SMS), typeof(MockSMS));
            cb.BindNamedParameter(namedParameter, "8675309");

            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var activityRef = (com.microsoft.reef.activity.IActivity)injector.GetInstance(activityInterfaceType1);
            var tweeter = (Com.Microsoft.Tang.Examples.Tweeter)injector.GetInstance(tweeterType);

            Assert.IsNotNull(activityRef);
            Assert.IsNotNull(tweeter);

            byte[] b = new byte[10];
            //activityRef.Call(b);
            tweeter.sendMessage();
        }

        [TestMethod]
        public void TestActivityWithBinding()
        {
            Type activityInterfaceType = typeof(com.microsoft.reef.activity.IActivity);
            var a = Assembly.Load(@"com.microsoft.reef.activity");
            var a1 = Assembly.Load(@"com.microsoft.reef.activityInterface");
            Type activityType = a.GetType("com.microsoft.reef.activity.HelloActivity");

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file2, file3 });
            cb.BindImplementation(activityInterfaceType, activityType);
            Type namedParameter = a1.GetType(@"com.microsoft.reef.driver.activity.ActivityConfigurationOptions+Identifier");
            cb.BindNamedParameter(namedParameter, "Hello Activity");

            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var activityRef = (com.microsoft.reef.activity.IActivity)injector.GetInstance(activityInterfaceType);
            Assert.IsNotNull(activityRef);

            byte[] b = new byte[10];
            //activityRef.Call(b);
        }

        [TestMethod]
        public void TestHelloStreamingActivityWithBinding()
        {
            Type activityInterfaceType = typeof(com.microsoft.reef.activity.IActivity);
            var a = Assembly.Load(@"com.microsoft.reef.activity");
            var a1 = Assembly.Load(@"com.microsoft.reef.activityInterface");

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { file2, file3 });

            Type activityType = a.GetType("com.microsoft.reef.activity.StreamActivity1");
            cb.BindImplementation(activityInterfaceType, activityType);
            Type namedParameterId = a1.GetType(@"com.microsoft.reef.driver.activity.ActivityConfigurationOptions+Identifier");
            cb.BindNamedParameter(namedParameterId, "Hello Stereaming");
            Type namedParameterIp = a.GetType(@"com.microsoft.reef.activity.StreamActivity1+IpAddress");
            cb.BindNamedParameter(namedParameterIp, "127.0.0.0");

            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var activityRef = (com.microsoft.reef.activity.IActivity)injector.GetInstance(activityInterfaceType);
            Assert.IsNotNull(activityRef);

            byte[] b = new byte[10];
            //activityRef.Call(b);
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

            var sms = (Com.Microsoft.Tang.Examples.SMS)injector.GetInstance(typeof(SMS));
            var factory = (Com.Microsoft.Tang.Examples.TweetFactory)injector.GetInstance(typeof(TweetFactory));
        }
    }
}
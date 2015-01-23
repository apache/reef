/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
﻿using System;
using System.Collections.Generic;
using System.Reflection;
﻿using Org.Apache.Reef.Tasks;
﻿using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Examples;
﻿using Org.Apache.Reef.Tang.Exceptions;
﻿using Org.Apache.Reef.Tang.Formats;
using Org.Apache.Reef.Tang.Implementations;
﻿using Org.Apache.Reef.Tang.Implementations.Configuration;
﻿using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Protobuf;
using Org.Apache.Reef.Tang.Util;
﻿using Org.Apache.Reef.Tang.Test.ScenarioTest;
﻿using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Org.Apache.Reef.Tang.Test.Configuration
{
    [TestClass]
    public class TestConfiguration
    {
        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
        }

        [TestMethod]
        public void TestDeserializedConfigMerge()
        {
            Type activityInterfaceType = typeof(ITask);
            ITang tang = TangFactory.GetTang();

            ICsConfigurationBuilder cb1 = tang.NewConfigurationBuilder();
            cb1.BindImplementation(GenericType<ITask>.Class, GenericType<HelloTask>.Class);
            cb1.BindNamedParameter<TaskConfigurationOptions.Identifier, string>(
                GenericType<TaskConfigurationOptions.Identifier>.Class, "Hello Task");
            IConfiguration conf1 = cb1.Build();
            var serializer = new AvroConfigurationSerializer();
            serializer.ToFile(conf1, "task.config");

            ICsConfigurationBuilder cb2 = tang.NewConfigurationBuilder();
            cb2.BindNamedParameter<Timer.Seconds, Int32>(GenericType<Timer.Seconds>.Class, "2");
            IConfiguration conf2 = cb2.Build();
            serializer.ToFile(conf2, "timer.config");

            ProtocolBufferClassHierarchy.Serialize("TaskTimer.bin", conf1.GetClassHierarchy());
            IClassHierarchy ns = ProtocolBufferClassHierarchy.DeSerialize("TaskTimer.bin");

            AvroConfiguration taskAvroconfiguration = serializer.AvroDeseriaizeFromFile("task.config");
            IConfiguration taskConfiguration = serializer.FromAvro(taskAvroconfiguration, ns);

            AvroConfiguration timerAvroconfiguration = serializer.AvroDeseriaizeFromFile("timer.config");
            IConfiguration timerConfiguration = serializer.FromAvro(timerAvroconfiguration, ns);

            IConfiguration merged = Configurations.MergeDeserializedConfs(taskConfiguration, timerConfiguration);

            var b = merged.newBuilder().Build();
        }

        [TestMethod]
        public void TestActivityConfiguration()
        {
            Type activityInterfaceType = typeof (ITask);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Common, FileNames.Tasks });
            cb.BindImplementation(GenericType<ITask>.Class, GenericType<HelloTask>.Class);
            cb.BindNamedParameter<TaskConfigurationOptions.Identifier, string>(
                GenericType<TaskConfigurationOptions.Identifier>.Class, "Hello Task");
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "TaskConf.txt");
            IDictionary<string, string> p = ConfigurationFile.FromFile("TaskConf.txt");

            ITang tang1 = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang1.NewConfigurationBuilder(new string[] { FileNames.Common, FileNames.Tasks });
            ConfigurationFile.AddConfigurationFromFile(cb1, "TaskConf.txt");
            IConfiguration conf1 = cb1.Build();

            IInjector injector = tang1.NewInjector(conf1);
            var activityRef = (ITask) injector.GetInstance(activityInterfaceType);
            Assert.IsNotNull(activityRef);
        }

        [TestMethod]
        public void TestMultipleConfiguration()
        {
            Type activityInterfaceType = typeof (ITask);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Common, FileNames.Tasks });
            cb.BindImplementation(GenericType<ITask>.Class, GenericType<HelloTask>.Class);
            cb.BindNamedParameter<TaskConfigurationOptions.Identifier, string>(
                GenericType<TaskConfigurationOptions.Identifier>.Class, "Hello Task");
            IConfiguration conf = cb.Build();

            IConfiguration httpConfiguraiton = HttpHandlerConfiguration.CONF
                .Set(HttpHandlerConfiguration.P, GenericType<HttpServerReefEventHandler>.Class)
                .Set(HttpHandlerConfiguration.P, GenericType<HttpServerNrtEventHandler>.Class)
                .Build();

            IInjector injector = TangFactory.GetTang().NewInjector(new IConfiguration[] {conf, httpConfiguraiton});
            var activityRef = (ITask) injector.GetInstance(activityInterfaceType);
            Assert.IsNotNull(activityRef);

            RuntimeClock clock = injector.GetInstance<RuntimeClock>();
            var rh = clock.ClockRuntimeStartHandler.Get();
            Assert.AreEqual(rh.Count, 1);
        }

        [TestMethod]
        public void TestActivityConfigWithSeperateAssembly()
        {
            Type activityInterfaceType = typeof (ITask);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Common, FileNames.Tasks });
            cb.BindImplementation(GenericType<ITask>.Class, GenericType<HelloTask>.Class);
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "TaskConf1.txt");
            IDictionary<string, string> p = ConfigurationFile.FromFile("TaskConf1.txt");

            IInjector injector = tang.NewInjector(new string[] { FileNames.Common, FileNames.Tasks }, "TaskConf1.txt");
            var activityRef = (ITask) injector.GetInstance(activityInterfaceType);

            //combined line sample
            var o = (ITask) TangFactory.GetTang()
                   .NewInjector(new string[] { FileNames.Common, FileNames.Tasks }, "TaskConf1.txt")
                   .GetInstance(typeof (ITask));

            Assert.IsNotNull(activityRef);
        }

        [TestMethod]
        public void TestGetConfgiFromProtoBufClassHierarchy()
        {
            Type iTaskType = typeof(Org.Apache.Reef.Tasks.ITask);
            Type helloTaskType = typeof(Org.Apache.Reef.Tasks.HelloTask);
            Type identifierType = typeof (TaskConfigurationOptions.Identifier);

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(new string[] { FileNames.Common, FileNames.Tasks });
            ProtocolBufferClassHierarchy.Serialize("Task.bin", ns);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize("Task.bin");
            ITang tang = TangFactory.GetTang();
            IConfigurationBuilder cb = tang.NewConfigurationBuilder(ch);
            cb.Bind(iTaskType.AssemblyQualifiedName, helloTaskType.AssemblyQualifiedName);
            cb.Bind(identifierType.AssemblyQualifiedName, "Hello Task");
            IConfiguration conf = cb.Build();
            ConfigurationFile.WriteConfigurationFile(conf, "taskConf2.txt");
        }

        [TestMethod]
        public void TestActivityConfig()
        {
            Type activityInterfaceType = typeof (ITask);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples, FileNames.Common, FileNames.Tasks });
            cb.BindImplementation(GenericType<ITask>.Class, GenericType<HelloTask>.Class);
            IConfiguration conf = cb.Build();
            ConfigurationFile.WriteConfigurationFile(conf, "TaskConf.txt");
            IDictionary<string, string> p = ConfigurationFile.FromFile("TaskConf.txt");

            IInjector injector = tang.NewInjector(new string[] { FileNames.Common, FileNames.Tasks }, "TaskConf.txt");
            var activityRef = (ITask) injector.GetInstance(activityInterfaceType);

            Assert.IsNotNull(activityRef);
        }

        [TestMethod]
        public void TestActivityConfigWithString()
        {
            Type activityInterfaceType = typeof (ITask);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples, FileNames.Common, FileNames.Tasks });
            cb.BindImplementation(GenericType<ITask>.Class, GenericType<HelloTask>.Class);
            IConfiguration conf = cb.Build();

            string s = ConfigurationFile.ToConfigurationString(conf);
            ICsConfigurationBuilder cb2 = tang.NewConfigurationBuilder(new string[] { FileNames.Examples, FileNames.Common, FileNames.Tasks });
            ConfigurationFile.AddConfigurationFromString(cb2, s);
            IConfiguration conf2 = cb2.Build();

            IInjector injector = tang.NewInjector(conf2);
            var activityRef = (ITask) injector.GetInstance(activityInterfaceType);

            Assert.IsNotNull(activityRef);
        }

        [TestMethod]
        public void TestTweetConfiguration()
        {
            Type tweeterType = typeof (Tweeter);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples });
            cb.BindImplementation(GenericType<ITweetFactory>.Class, GenericType<MockTweetFactory>.Class);
            cb.BindImplementation(GenericType<ISMS>.Class, GenericType<MockSMS>.Class);
            cb.BindNamedParameter<Tweeter.PhoneNumber, long>(GenericType<Tweeter.PhoneNumber>.Class, "8675309");
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "tweeterConf.txt");
            IDictionary<string, string> p = ConfigurationFile.FromFile("tweeterConf.txt");
            ITang tang1 = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang1.NewConfigurationBuilder(new string[] { FileNames.Examples });
            ConfigurationFile.AddConfigurationFromFile(cb1, "tweeterConf.txt");
            IConfiguration conf1 = cb1.Build();

            IInjector injector = tang1.NewInjector(conf1);
            var tweeter = (Tweeter) injector.GetInstance(tweeterType);
            tweeter.sendMessage();
        }

        [TestMethod]
        public void TestTweetConfig()
        {
            Type tweeterType = typeof (Tweeter);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples });
            cb.BindImplementation(GenericType<ITweetFactory>.Class, GenericType<MockTweetFactory>.Class);
            cb.BindImplementation(GenericType<ISMS>.Class, GenericType<MockSMS>.Class);
            cb.BindNamedParameter<Tweeter.PhoneNumber, long>(GenericType<Tweeter.PhoneNumber>.Class, "8675309");
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "tweeterConf.txt");
            IDictionary<string, string> p = ConfigurationFile.FromFile("tweeterConf.txt");

            IInjector injector = tang.NewInjector(new string[] { FileNames.Examples }, "tweeterConf.txt");
            var tweeter = (Tweeter) injector.GetInstance(tweeterType);
            tweeter.sendMessage();
        }


        [TestMethod]
        public void TestTweetConfigWithAvroThroughFile()
        {
            Type tweeterType = typeof (Tweeter);
            ITang tang = TangFactory.GetTang();
            IConfiguration conf = tang.NewConfigurationBuilder(new string[] { FileNames.Examples })
                                      .BindImplementation(GenericType<ITweetFactory>.Class,
                                                          GenericType<MockTweetFactory>.Class)
                                      .BindImplementation(GenericType<ISMS>.Class, GenericType<MockSMS>.Class)
                                      .BindNamedParameter<Tweeter.PhoneNumber, long>(
                                          GenericType<Tweeter.PhoneNumber>.Class, "8675309")
                                      .Build();

            var serializer = new AvroConfigurationSerializer();
            serializer.ToFileStream(conf, "tweeterConfAvro.bin");
            IConfiguration conf2 = serializer.FromFileStream("tweeterConfAvro.bin");

            IInjector injector = tang.NewInjector(conf2);
            var tweeter = (Tweeter) injector.GetInstance(tweeterType);
            tweeter.sendMessage();
        }

        [TestMethod]
        public void TestTweetConfigAddConfigurationFromString()
        {
            Type tweeterType = typeof (Tweeter);
            ITang tang = TangFactory.GetTang();
            IConfiguration conf = tang.NewConfigurationBuilder(new string[] { FileNames.Examples })
                                      .BindImplementation(GenericType<ITweetFactory>.Class,
                                                          GenericType<MockTweetFactory>.Class)
                                      .BindImplementation(GenericType<ISMS>.Class, GenericType<MockSMS>.Class)
                                      .BindNamedParameter<Tweeter.PhoneNumber, long>(
                                          GenericType<Tweeter.PhoneNumber>.Class, "8675309")
                                      .Build();

            ConfigurationFile.WriteConfigurationFile(conf, "tweeterConf.txt");
            string s = ConfigurationFile.ToConfigurationString(conf);
            ICsConfigurationBuilder cb2 = tang.NewConfigurationBuilder();
            ConfigurationFile.AddConfigurationFromString(cb2, s);
            IConfiguration conf2 = cb2.Build();

            IInjector injector = tang.NewInjector(conf2);
            var tweeter = (Tweeter) injector.GetInstance(tweeterType);
            tweeter.sendMessage();
        }

        [TestMethod]
        public void TestTweetConfigWithAvroSerialization()
        {
            Type tweeterType = typeof (Tweeter);
            ITang tang = TangFactory.GetTang();
            IConfiguration conf = tang.NewConfigurationBuilder(new string[] { FileNames.Examples })
                                      .BindImplementation(GenericType<ITweetFactory>.Class,
                                                          GenericType<MockTweetFactory>.Class)
                                      .BindImplementation(GenericType<ISMS>.Class, GenericType<MockSMS>.Class)
                                      .BindNamedParameter<Tweeter.PhoneNumber, long>(
                                          GenericType<Tweeter.PhoneNumber>.Class, "8675309")
                                      .Build();

            var serializer = new AvroConfigurationSerializer();
            byte[] bytes = serializer.ToByteArray(conf);
            IConfiguration conf2 = serializer.FromByteArray(bytes);

            IInjector injector = tang.NewInjector(conf2);
            var tweeter = (Tweeter) injector.GetInstance(tweeterType);
            tweeter.sendMessage();
        }

        [TestMethod]
        public void TestTweetConfigGetConfigurationFromString()
        {
            Type tweeterType = typeof (Tweeter);
            ITang tang = TangFactory.GetTang();
            IConfiguration conf = tang.NewConfigurationBuilder(new string[] { FileNames.Examples })
                                      .BindImplementation(GenericType<ITweetFactory>.Class,
                                                          GenericType<MockTweetFactory>.Class)
                                      .BindImplementation(GenericType<ISMS>.Class, GenericType<MockSMS>.Class)
                                      .BindNamedParameter<Tweeter.PhoneNumber, long>(
                                          GenericType<Tweeter.PhoneNumber>.Class, "8675309")
                                      .Build();

            ConfigurationFile.WriteConfigurationFile(conf, "tweeterConf.txt");
            string s = ConfigurationFile.ToConfigurationString(conf);
            IConfiguration conf2 = ConfigurationFile.GetConfiguration(s);

            IInjector injector = tang.NewInjector(conf2);
            var tweeter = (Tweeter) injector.GetInstance(tweeterType);
            tweeter.sendMessage();
        }

        [TestMethod]
        public void TestTweetInvalidBinding()
        {
            string msg = null;
            try
            {
                TangFactory.GetTang().NewConfigurationBuilder(new string[] { FileNames.Examples })
                           .BindImplementation(typeof (ITweetFactory), typeof (MockSMS))
                           .Build();
            }
            catch (ArgumentException e)
            {
                msg = e.Message;
            }
            Assert.IsNotNull(msg);
        }

        [TestMethod]
        public void TestTimerConfiguration()
        {
            Type timerType = typeof (Timer);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples });
            cb.BindNamedParameter<Timer.Seconds, Int32>(GenericType<Timer.Seconds>.Class, "2");
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "timerConf.txt");
            IDictionary<string, string> p = ConfigurationFile.FromFile("timerConf.txt");

            ITang tang1 = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang1.NewConfigurationBuilder(new string[] { FileNames.Examples });
            ConfigurationFile.AddConfigurationFromFile(cb1, "timerConf.txt");
            IConfiguration conf1 = cb1.Build();

            IInjector injector = tang.NewInjector(conf1);
            var timer = (Timer) injector.GetInstance(timerType);

            Assert.IsNotNull(timer);

            timer.sleep();
        }

        [TestMethod]
        public void TestDocumentLoadNamedParameterConfiguration()
        {
            Type documentedLocalNamedParameterType = typeof (DocumentedLocalNamedParameter);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples });
            cb.BindNamedParameter<DocumentedLocalNamedParameter.Foo, string>(
                GenericType<DocumentedLocalNamedParameter.Foo>.Class, "Hello");
            IConfiguration conf = cb.Build();

            ConfigurationFile.WriteConfigurationFile(conf, "docLoadConf.txt");
            IDictionary<string, string> p = ConfigurationFile.FromFile("docLoadConf.txt");

            ITang tang1 = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang1.NewConfigurationBuilder(new string[] { FileNames.Examples });
            ConfigurationFile.AddConfigurationFromFile(cb1, "docLoadConf.txt");
            IConfiguration conf1 = cb1.Build();

            IInjector injector = tang1.NewInjector(conf1);
            var doc = (DocumentedLocalNamedParameter) injector.GetInstance(documentedLocalNamedParameterType);

            Assert.IsNotNull(doc);
            var s = doc.ToString();
        }

        [TestMethod]
        public void TestTimerConfigurationWithClassHierarchy()
        {
            Type timerType = typeof (Timer);
            ClassHierarchyImpl classHierarchyImpl = new ClassHierarchyImpl(FileNames.Examples);

            ITang tang = TangFactory.GetTang();
            IConfiguration conf = tang.NewConfigurationBuilder(classHierarchyImpl)
                                      .BindNamedParameter<Timer.Seconds, Int32>(GenericType<Timer.Seconds>.Class, "1")
                                      .Build();

            ConfigurationFile.WriteConfigurationFile(conf, "timerConfH.txt");
            IDictionary<string, string> p = ConfigurationFile.FromFile("timerConfH.txt");

            ITang tang1 = TangFactory.GetTang();
            ICsConfigurationBuilder cb1 = tang1.NewConfigurationBuilder(new string[] { FileNames.Examples });
            ConfigurationFile.AddConfigurationFromFile(cb1, "timerConfH.txt");
            IConfiguration conf1 = cb1.Build();

            IInjector injector = tang1.NewInjector(conf1);
            var timer = (Timer) injector.GetInstance(timerType);

            Assert.IsNotNull(timer);
            timer.sleep();
        }

        [TestMethod]
        public void TestSetConfig()
        {
            IConfiguration conf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<SetOfNumbers, string>(GenericType<SetOfNumbers>.Class, "four")
                .BindSetEntry<SetOfNumbers, string>(GenericType<SetOfNumbers>.Class, "five")
                .BindSetEntry<SetOfNumbers, string>(GenericType<SetOfNumbers>.Class, "six")
                .Build();

            Box b = (Box) TangFactory.GetTang().NewInjector(conf).GetInstance(typeof (Box));
            ConfigurationFile.WriteConfigurationFile(conf, "SetOfNumbersConf.txt");

            string s = ConfigurationFile.ToConfigurationString(conf);
            IConfiguration conf2 = ConfigurationFile.GetConfiguration(s);

            Box b2 = (Box) TangFactory.GetTang().NewInjector(conf2).GetInstance(typeof (Box));
            ISet<string> actual = b2.Numbers;

            Assert.IsTrue(actual.Contains("four"));
            Assert.IsTrue(actual.Contains("five"));
            Assert.IsTrue(actual.Contains("six"));
        }

        [TestMethod]
        public void TestSetConfigWithAvroSerialization()
        {
            IConfiguration conf = TangFactory.GetTang().NewConfigurationBuilder()
                    .BindSetEntry<SetOfNumbers, string>(GenericType<SetOfNumbers>.Class, "four")
                    .BindSetEntry<SetOfNumbers, string>(GenericType<SetOfNumbers>.Class, "five")
                    .BindSetEntry<SetOfNumbers, string>(GenericType<SetOfNumbers>.Class, "six")
                    .Build();

            Box b = (Box) TangFactory.GetTang().NewInjector(conf).GetInstance(typeof (Box));

            var serializer = new AvroConfigurationSerializer();
            byte[] bytes = serializer.ToByteArray(conf);
            IConfiguration conf2 = serializer.FromByteArray(bytes);

            Box b2 = (Box) TangFactory.GetTang().NewInjector(conf2).GetInstance(typeof (Box));
            ISet<string> actual = b2.Numbers;

            Assert.IsTrue(actual.Contains("four"));
            Assert.IsTrue(actual.Contains("five"));
            Assert.IsTrue(actual.Contains("six"));
        }

        [TestMethod]
        public void TestNullStringVaue()
        {
            string msg = null;
            try
            {
                TangFactory.GetTang().NewConfigurationBuilder()
                    .BindNamedParameter<NamedParamterNoDefault.NamedString, string>(GenericType<NamedParamterNoDefault.NamedString>.Class, null)
                    .Build();
            }
            catch (IllegalStateException e)
            {
                msg = e.Message;
            }
            Assert.IsNotNull(msg);
        }

        [TestMethod]
        public void TestSetConfigNullValue()
        {
            string msg = null;
            try
            {
                TangFactory.GetTang().NewConfigurationBuilder()
                    .BindSetEntry<SetOfNumbersNoDefault, string>(GenericType<SetOfNumbersNoDefault>.Class, null)
                    .BindSetEntry<SetOfNumbersNoDefault, string>(GenericType<SetOfNumbersNoDefault>.Class, "five")
                    .BindSetEntry<SetOfNumbersNoDefault, string>(GenericType<SetOfNumbersNoDefault>.Class, "six")
                    .Build();
            }
            catch (IllegalStateException e)
            {
                msg = e.Message;
            }
            Assert.IsNotNull(msg);
        }
    }

    [NamedParameter(DefaultValues = new string[] {"one", "two", "three"})]
    class SetOfNumbers : Name<ISet<string>>
    {
    }

    class Box
    {
        public ISet<string> Numbers;

        [Inject]
        Box([Parameter(typeof (SetOfNumbers))] ISet<string> numbers)
        {
            this.Numbers = numbers;
        }
    }

    [NamedParameter]
    class SetOfNumbersNoDefault : Name<ISet<string>>
    {
    }

    class BoxNoDefault
    {
        public ISet<string> Numbers;

        [Inject]
        BoxNoDefault([Parameter(typeof(SetOfNumbersNoDefault))] ISet<string> numbers)
        {
            this.Numbers = numbers;
        }
    }

    class NamedParamterNoDefault
    {
        private string str;

        [NamedParameter]
        public class NamedString : Name<string>
        {
        }

        [Inject]
        NamedParamterNoDefault([Parameter(typeof (NamedString))] string str)
        {
            this.str = str;
        }

        public string GetString()
        {
            return str;
        }
    }
}
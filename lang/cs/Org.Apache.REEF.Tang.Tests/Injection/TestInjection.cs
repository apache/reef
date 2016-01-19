// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Examples.Tasks.StreamingTasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Examples;
using Org.Apache.REEF.Tang.Implementations.ClassHierarchy;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Tests.Injection
{
    [DefaultImplementation(typeof(AReferenceClass))]
    internal interface IAInterface
    {
    }

    [TestClass]
    public class TestInjection
    {
        static Assembly asm = null;

        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            asm = Assembly.Load(FileNames.Examples);
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
        }

        [TestInitialize]
        public void TestSetup()
        {
        }

        [TestCleanup]
        public void TestCleanup()
        {
        }

        [TestMethod]
        public void TestTimer()
        {
            Type timerType = typeof(Timer);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples });
            cb.BindNamedParameter<Timer.Seconds, int>(GenericType<Timer.Seconds>.Class, "2");
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var timer = (Timer)injector.GetInstance(timerType);

            Assert.IsNotNull(timer);

            timer.sleep();
        }

        [TestMethod]
        public void TestTimerWithClassHierarchy()
        {
            Type timerType = typeof(Timer);

            ClassHierarchyImpl classHierarchyImpl = new ClassHierarchyImpl(FileNames.Examples);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder((ICsClassHierarchy)classHierarchyImpl);

            cb.BindNamedParameter<Timer.Seconds, int>(GenericType<Timer.Seconds>.Class, "2");
            IConfiguration conf = cb.Build();

            IInjector injector = tang.NewInjector(conf);
            var timer = (Timer)injector.GetInstance(timerType);

            Assert.IsNotNull(timer);

            timer.sleep();
        }

        [TestMethod]
        public void TestDocumentLoadNamedParameter()
        {
            Type documentedLocalNamedParameterType = typeof(DocumentedLocalNamedParameter);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples });
            cb.BindNamedParameter<DocumentedLocalNamedParameter.Foo, string>(GenericType<DocumentedLocalNamedParameter.Foo>.Class, "Hello");
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var doc = (DocumentedLocalNamedParameter)injector.GetInstance(documentedLocalNamedParameterType);

            Assert.IsNotNull(doc);
        }

        [TestMethod]
        public void TestDocumentLoadNamedParameterWithDefaultValue()
        {
            ITang tang = TangFactory.GetTang();
            IConfiguration conf = tang.NewConfigurationBuilder(new string[] { FileNames.Examples }).Build();
            IInjector injector = tang.NewInjector(conf);
            var doc = (DocumentedLocalNamedParameter)injector.GetInstance(typeof(DocumentedLocalNamedParameter));

            Assert.IsNotNull(doc);
        }

        [TestMethod]
        public void TestSimpleConstructor()
        {
            Type simpleConstructorType = typeof(SimpleConstructors);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples });
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var simpleConstructor = (SimpleConstructors)injector.GetInstance(simpleConstructorType);
            Assert.IsNotNull(simpleConstructor);
        }

        [TestMethod]
        public void TestActivity()
        {
            Type activityType = typeof(HelloTask);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Tasks, FileNames.Common });
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var activityRef = (ITask)injector.GetInstance(activityType);
            Assert.IsNotNull(activityRef);
        }

        [TestMethod]
        public void TestStreamActivity1()
        {
            Type activityType = typeof(StreamTask1);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Tasks, FileNames.Common });
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var activityRef = (ITask)injector.GetInstance(activityType);
            Assert.IsNotNull(activityRef);
        }

        [TestMethod]
        public void TestStreamActivity2()
        {
            Type activityType = typeof(StreamTask2);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Tasks, FileNames.Common });
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var activityRef = (ITask)injector.GetInstance(activityType);
            Assert.IsNotNull(activityRef);
        }

        [TestMethod]
        public void TestMultipleAssemlies()
        {
            Type activityInterfaceType1 = typeof(ITask);
            Type tweeterType = typeof(Tweeter);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples, FileNames.Tasks, FileNames.Common });
            cb.BindImplementation(GenericType<ITask>.Class, GenericType<HelloTask>.Class);
            cb.BindImplementation(GenericType<ITweetFactory>.Class, GenericType<MockTweetFactory>.Class);
            cb.BindImplementation(GenericType<ISMS>.Class, GenericType<MockSMS>.Class);
            cb.BindNamedParameter<Tweeter.PhoneNumber, long>(GenericType<Tweeter.PhoneNumber>.Class, "8675309");

            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var activityRef = (ITask)injector.GetInstance(activityInterfaceType1);
            var tweeter = (Tweeter)injector.GetInstance(tweeterType);

            Assert.IsNotNull(activityRef);
            Assert.IsNotNull(tweeter);

            tweeter.sendMessage();
        }

        [TestMethod]
        public void TestActivityWithBinding()
        {
            Type activityInterfaceType = typeof(ITask);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Tasks, FileNames.Common });
            cb.BindImplementation(GenericType<ITask>.Class, GenericType<HelloTask>.Class);
            cb.BindNamedParameter<TaskConfigurationOptions.Identifier, string>(GenericType<TaskConfigurationOptions.Identifier>.Class, "Hello Task");

            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            ITask activityRef1 = injector.GetInstance<ITask>();
            var activityRef2 = (ITask)injector.GetInstance(activityInterfaceType);
            Assert.IsNotNull(activityRef2);
            Assert.IsNotNull(activityRef1);
            Assert.AreEqual(activityRef1, activityRef2);
        }

        [TestMethod]
        public void TestHelloStreamingActivityWithBinding()
        {
            Type activityInterfaceType = typeof(ITask);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Tasks, FileNames.Common });

            cb.BindImplementation(GenericType<ITask>.Class, GenericType<HelloTask>.Class);
            cb.BindNamedParameter<TaskConfigurationOptions.Identifier, string>(GenericType<TaskConfigurationOptions.Identifier>.Class, "Hello Stereamingk");
            cb.BindNamedParameter<StreamTask1.IpAddress, string>(GenericType<StreamTask1.IpAddress>.Class, "127.0.0.0");
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            var activityRef = (ITask)injector.GetInstance(activityInterfaceType);
            Assert.IsNotNull(activityRef);
        }

        [TestMethod]
        public void TestTweetExample()
        {
            Type tweeterType = typeof(Tweeter);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples });

            IConfiguration conf = cb.BindImplementation(GenericType<ITweetFactory>.Class, GenericType<MockTweetFactory>.Class)
            .BindImplementation(GenericType<ISMS>.Class, GenericType<MockSMS>.Class)
            .BindNamedParameter<Tweeter.PhoneNumber, long>(GenericType<Tweeter.PhoneNumber>.Class, "8675309")
            .Build();
            IInjector injector = tang.NewInjector(conf);
            var tweeter = (Tweeter)injector.GetInstance(tweeterType);
            tweeter.sendMessage();

            var sms = (ISMS)injector.GetInstance(typeof(ISMS));
            var factory = (ITweetFactory)injector.GetInstance(typeof(ITweetFactory));
            Assert.IsNotNull(sms);
            Assert.IsNotNull(factory);
        }

        [TestMethod]
        public void TestReferenceType()
        {
            AReferenceClass o = (AReferenceClass)TangFactory.GetTang().NewInjector().GetInstance(typeof(IAInterface));
        }

        [TestMethod]
        public void TestGeneric()
        {
            var o = (AGenericClass<int>)TangFactory.GetTang().NewInjector().GetInstance(typeof(AGenericClass<int>));
            var o2 = (AClassWithGenericArgument<int>)TangFactory.GetTang().NewInjector().GetInstance(typeof(AClassWithGenericArgument<int>));
            Assert.IsNotNull(o);
            Assert.IsNotNull(o2);
        }

        [TestMethod]
        public void TestNestedClass()
        {
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();

            IConfiguration conf = cb
            .BindNamedParameter<ClassParameter.Named1, int>(GenericType<ClassParameter.Named1>.Class, "5")
            .BindNamedParameter<ClassParameter.Named2, string>(GenericType<ClassParameter.Named2>.Class, "hello")
            .Build();

            IInjector injector = tang.NewInjector(conf);
            ClassHasNestedClass h = injector.GetInstance<ClassHasNestedClass>();

            Assert.IsNotNull(h);
        }

        [TestMethod]
        public void TestExternalObject()
        {
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();

            IInjector injector = tang.NewInjector(cb.Build());

            // bind an object to the injetor so that Tang will get this instance from cache directly instead of inject it when injecting ClassWithExternalObject
            injector.BindVolatileInstance(GenericType<ExternalClass>.Class, new ExternalClass());
            ClassWithExternalObject o = injector.GetInstance<ClassWithExternalObject>();

            Assert.IsNotNull(o.ExternalObject is ExternalClass);
        }

        /// <summary>
        /// In this test, interface is a generic of T. Implementations have different generic arguments such as int and string. 
        /// When doing injection, we must specify the interface with a specified argument type
        /// </summary>
        [TestMethod]
        public void TestInjectionWithGenericArguments()
        {
            var c = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IMyOperator<int>>.Class, GenericType<MyOperatorImpl<int>>.Class)
                .BindImplementation(GenericType<IMyOperator<string>>.Class, GenericType<MyOperatorImpl<string>>.Class)
                .Build();

            var injector = TangFactory.GetTang().NewInjector(c);

            // argument type must be specified in injection
            var o1 = injector.GetInstance(typeof(IMyOperator<int>));
            var o2 = injector.GetInstance(typeof(IMyOperator<string>));
            var o3 = injector.GetInstance(typeof(MyOperatorTopology<int>));

            Assert.IsTrue(o1 is MyOperatorImpl<int>);
            Assert.IsTrue(o2 is MyOperatorImpl<string>);
            Assert.IsTrue(o3 is MyOperatorTopology<int>);
        }

        /// <summary>
        /// In this test, interface argument type is set through Configuration. We can get the argument type and then 
        /// make the interface with the argument type on the fly so that to do the injection
        /// </summary>
        [TestMethod]
        public void TestInjectionWithGenericArgumentType()
        {
            var c = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IMyOperator<int[]>>.Class, GenericType<MyOperatorImpl<int[]>>.Class)
                .BindNamedParameter(typeof(MessageType), typeof(int[]).AssemblyQualifiedName)
                .Build();

            var injector = TangFactory.GetTang().NewInjector(c);

            // get argument type from configuration
            var messageTypeAsString = injector.GetNamedInstance<MessageType, string>(GenericType<MessageType>.Class);
            Type messageType = Type.GetType(messageTypeAsString);

            // create interface with generic type on the fly
            Type genericInterfaceType = typeof(IMyOperator<>);
            Type interfaceOfMessageType = genericInterfaceType.MakeGenericType(messageType);

            var o = injector.GetInstance(interfaceOfMessageType);

            Assert.IsTrue(o is MyOperatorImpl<int[]>);
        }
    }

    class AReferenceClass : IAInterface
    {
        [Inject]
        public AReferenceClass(AReferenced refclass)
        {            
        }
    }

    class AReferenced
    {
        [Inject]
        public AReferenced()
        {
        }
    }

    class AGenericClass<T>
    {
        [Inject]
        public AGenericClass()
        {
        }
    }

    class AClassWithGenericArgument<T>
    {
        [Inject]
        public AClassWithGenericArgument(AGenericClass<T> g)
        {           
        }
    }

    class ClassHasNestedClass
    {
        [Inject]
        public ClassHasNestedClass(ClassParameter h1)
        {
        }
    }

    class ClassParameter
    {        
        private int i;
        private string s;

        [Inject]
        public ClassParameter([Parameter(typeof(Named1))] int i, [Parameter(typeof(Named2))] string s)
        {
            this.i = i;
            this.s = s;
        }

        [NamedParameter]
        public class Named1 : Name<int>
        {
        }
        
        [NamedParameter]
        public class Named2 : Name<string>
        {            
        }
    }

    class ClassWithExternalObject
    {
        [Inject]
        public ClassWithExternalObject(ExternalClass ec)
        {
            ExternalObject = ec;
        }

        public ExternalClass ExternalObject { get; set; }
    }

    class ExternalClass
    {
        public ExternalClass()
        {            
        }
    }

    interface IMyOperator<T>
    {
        string OperatorName { get; } 
    }

    class MyOperatorImpl<T> : IMyOperator<T>
    {
        [Inject]
        public MyOperatorImpl()
        {           
        }

        string IMyOperator<T>.OperatorName
        {
            get { throw new NotImplementedException(); }
        }
    }

    [NamedParameter]
    class MessageType : Name<string>
    {        
    }

    class MyOperatorTopology<T>
    {
        [Inject]
        public MyOperatorTopology(IMyOperator<T> op)
        {           
        }
    }
}
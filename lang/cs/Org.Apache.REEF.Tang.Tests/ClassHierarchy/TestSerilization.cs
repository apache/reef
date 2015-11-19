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

using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Examples.Tasks.StreamingTasks;
using Org.Apache.REEF.Tang.Examples;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Protobuf;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Tests.ClassHierarchy
{
    [TestClass]
    public class TestSerilization
    {
        static Assembly asm = null;

        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            asm = Assembly.Load(FileNames.Examples);
            Assembly.Load(FileNames.Examples);
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
            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(new string[] { typeof(Timer).Assembly.GetName().Name });
            ProtocolBufferClassHierarchy.Serialize("node.bin", ns);
        }

        [TestMethod]
        public void TestDeSerializeClassHierarchy()
        {
            Type timerType = typeof(Timer);
            Type SecondType = typeof(Timer.Seconds);
            Type simpleCOnstuctorType = typeof(SimpleConstructors);

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(new string[] { typeof(Timer).Assembly.GetName().Name });
            IClassNode timerClassNode = (IClassNode)ns.GetNode(timerType.AssemblyQualifiedName);
            INode secondNode = (INode)ns.GetNode(SecondType.AssemblyQualifiedName);
            IClassNode SimpleConstructorsClassNode = (IClassNode)ns.GetNode(simpleCOnstuctorType.AssemblyQualifiedName);

            ProtocolBufferClassHierarchy.Serialize("node.bin", ns);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize("node.bin");

            IClassNode timerClassNode2 = (IClassNode)ch.GetNode(timerType.AssemblyQualifiedName);
            INode secondNode2 = ch.GetNode(SecondType.AssemblyQualifiedName);
            IClassNode SimpleConstructorsClassNode2 = (IClassNode)ch.GetNode(simpleCOnstuctorType.AssemblyQualifiedName);

            Assert.AreEqual(timerClassNode.GetFullName(), timerClassNode2.GetFullName());
            Assert.AreEqual(secondNode.GetFullName(), secondNode2.GetFullName());
            Assert.AreEqual(SimpleConstructorsClassNode.GetFullName(), SimpleConstructorsClassNode2.GetFullName());

            Assert.IsTrue(SimpleConstructorsClassNode2.GetChildren().Count == 0);
            IList<IConstructorDef> def = SimpleConstructorsClassNode2.GetInjectableConstructors();
            Assert.AreEqual(3, def.Count);
        }

        [TestMethod]
        public void TestDeSerializeClassHierarchyForTask()
        {
            Type streamTask1Type = typeof(StreamTask1);
            Type helloTaskType = typeof(HelloTask);

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(new string[] { typeof(HelloTask).Assembly.GetName().Name });
            IClassNode StreamTask1ClassNode = (IClassNode)ns.GetNode(streamTask1Type.AssemblyQualifiedName);
            IClassNode HelloTaskClassNode = (IClassNode)ns.GetNode(helloTaskType.AssemblyQualifiedName);

            ProtocolBufferClassHierarchy.Serialize("task.bin", ns);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize("task.bin");
            IClassNode StreamTask1ClassNode2 = (IClassNode)ch.GetNode(streamTask1Type.AssemblyQualifiedName);
            IClassNode HelloTaskClassNode2 = (IClassNode)ch.GetNode(helloTaskType.AssemblyQualifiedName);

            Assert.AreEqual(StreamTask1ClassNode.GetFullName(), StreamTask1ClassNode2.GetFullName());
            Assert.AreEqual(HelloTaskClassNode.GetFullName(), HelloTaskClassNode2.GetFullName());
        }

        [TestMethod]
        [DeploymentItem(@".")]
        public void TestDeSerializeClassHierarchyFromJava()
        {
            // the file comes from Java TestClassHierarchyRoundTrip SetUp3 testSimpleConstructors
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize("simpleConstructorJavaProto.bin");
            IClassNode simpleConstructorNode = (IClassNode)ch.GetNode("org.apache.reef.tang.implementation.SimpleConstructors");
            Assert.AreEqual(simpleConstructorNode.GetChildren().Count, 0);
            Assert.AreEqual(simpleConstructorNode.GetInjectableConstructors().Count, 3);
        }

        [TestMethod]
        public void TestSerializeClassHierarchyForAvro()
        {
            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(new string[] { typeof(Microsoft.Hadoop.Avro.AvroSerializer).Assembly.GetName().Name });
            Assert.IsNotNull(ns);
            ProtocolBufferClassHierarchy.Serialize("avro.bin", ns);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize("avro.bin");
            Assert.IsNotNull(ch);
        }

        [TestMethod]
        public void TestDeSerializeClassHierarchyAndBind()
        {
            Type streamTask1Type = typeof(StreamTask1);
            Type helloTaskType = typeof(HelloTask);

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(new string[] { typeof(HelloTask).Assembly.GetName().Name });
            IClassNode StreamTask1ClassNode = (IClassNode)ns.GetNode(streamTask1Type.AssemblyQualifiedName);
            IClassNode HelloTaskClassNode = (IClassNode)ns.GetNode(helloTaskType.AssemblyQualifiedName);

            ProtocolBufferClassHierarchy.Serialize("task.bin", ns);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize("task.bin");
            IClassNode StreamTask1ClassNode2 = (IClassNode)ch.GetNode(streamTask1Type.AssemblyQualifiedName);
            IClassNode HelloTaskClassNode2 = (IClassNode)ch.GetNode(helloTaskType.AssemblyQualifiedName);

            Assert.AreEqual(StreamTask1ClassNode.GetName(), StreamTask1ClassNode2.GetName());
            Assert.AreEqual(HelloTaskClassNode.GetName(), HelloTaskClassNode2.GetName());

            // have to use original class hierarchy for the merge. ClassHierarchy from ProtoBuffer doesn't support merge. 
            IConfigurationBuilder cb = TangFactory.GetTang()
                  .NewConfigurationBuilder(ns);
            cb.AddConfiguration(TaskConfiguration.ConfigurationModule
             .Set(TaskConfiguration.Identifier, "Hello_From_Streaming1")
             .Set(TaskConfiguration.Task, GenericType<StreamTask1>.Class)
             .Build());

            IConfiguration taskConfiguration = cb.Build();
            StreamTask1 st = TangFactory.GetTang().NewInjector(taskConfiguration).GetInstance<StreamTask1>();
            Assert.IsNotNull(st);
        }

        [TestMethod]
        public void TestSerirializeInjectionPlanForTimer()
        {
            Type timerType = typeof(Timer);
            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples });
            cb.BindNamedParameter<Timer.Seconds, Int32>(GenericType<Timer.Seconds>.Class, "2");
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan ip = injector.GetInjectionPlan(timerType);
            ProtocolBufferInjectionPlan.Serialize("timerplan.bin", ip);
            var ch = conf.GetClassHierarchy();
            var ip1 = ProtocolBufferInjectionPlan.DeSerialize("timerplan.bin", ch);
            Assert.IsNotNull(ip1);
        }

        [TestMethod]
        public void TestSerirializeInjectionPlanForSimpleConstructor()
        {
            Type simpleConstructorType = typeof(SimpleConstructors);

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { FileNames.Examples });
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);
            Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan ip = injector.GetInjectionPlan(simpleConstructorType);

            ProtocolBufferInjectionPlan.Serialize("plan.bin", ip);
            var ch = conf.GetClassHierarchy();
            var ipRecovered = ProtocolBufferInjectionPlan.DeSerialize("plan.bin", ch);
            Assert.IsNotNull(ipRecovered);
        }

        [TestMethod]
        public void TestGenericClass()
        {
            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(new string[] { typeof(Timer).Assembly.GetName().Name });

            Type t = typeof(Timer);
            IClassNode EventClassNode = (IClassNode)ns.GetNode(t.AssemblyQualifiedName);
            ProtocolBufferClassHierarchy.Serialize("event.bin", ns);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize("event.bin");
            IClassNode EventClassNode1 = (IClassNode)ns.GetNode(t.AssemblyQualifiedName);
            Assert.AreEqual(EventClassNode.GetName(), EventClassNode1.GetName());
        }

        [TestMethod]
        public void TestGenericArgument()
        {
            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(new string[] { typeof(ClassWithGenericArgument<>).Assembly.GetName().Name });

            Type t = typeof(ClassWithGenericArgument<>);
            IClassNode classNode = (IClassNode)ns.GetNode(t.AssemblyQualifiedName);
            var cons = classNode.GetAllConstructors();
            foreach (var c in cons)
            {
                var args = c.GetArgs();
                foreach (var a in args)
                {
                    Assert.IsNotNull(a.GetName());
                }
            }
            ProtocolBufferClassHierarchy.Serialize("generic.bin", ns);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize("generic.bin");
            IClassNode classNode1 = (IClassNode)ns.GetNode(t.AssemblyQualifiedName);
            Assert.AreEqual(classNode.GetName(), classNode1.GetName());
        }
    }
}
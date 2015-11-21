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
using System.IO;
using Microsoft.Hadoop.Avro;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Tang.Examples;
using Org.Apache.REEF.Tang.Implementations.ClassHierarchy;
using Org.Apache.REEF.Tang.Implementations.ClassHierarchy.AvroDataContract;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;

namespace Org.Apache.REEF.Tang.Tests.ClassHierarchy
{
    [TestClass]
    public class TestAvroSerialization
    {
        private AvroClassHierarchySerializer _serializer =
            (AvroClassHierarchySerializer)TangFactory.GetTang().NewInjector().GetInstance<IClassHierarchySerializer>();

        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
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

        /// <summary>
        /// This test is to convert an ClassHierarchy into AvroNode object, and then convert it back to an 
        /// AvroClassHierarchy. 
        /// </summary>
        [TestMethod]
        public void TestToFromAvroNode()
        {
            Type timerType = typeof(Timer);
            Type secondType = typeof(Timer.Seconds);
            Type simpleConstructorType = typeof(SimpleConstructors);

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(
                new string[] { typeof(Timer).Assembly.GetName().Name, typeof(SimpleConstructors).Assembly.GetName().Name });
            IClassNode timerClassNode = (IClassNode)ns.GetNode(timerType.AssemblyQualifiedName);
            INode secondNode = ns.GetNode(secondType.AssemblyQualifiedName);
            IClassNode simpleConstructorsClassNode = (IClassNode)ns.GetNode(simpleConstructorType.AssemblyQualifiedName);

            AvroNode n = _serializer.ToAvroNode(ns);
            IClassHierarchy ns2 = _serializer.FromAvroNode(n);

            IClassNode timerClassNode2 = (IClassNode)ns2.GetNode(timerType.AssemblyQualifiedName);
            INode secondNode2 = ns2.GetNode(secondType.AssemblyQualifiedName);
            IClassNode simpleConstructorsClassNode2 = (IClassNode)ns2.GetNode(simpleConstructorType.AssemblyQualifiedName);

            Assert.AreEqual(timerClassNode, timerClassNode2);
            Assert.AreEqual(secondNode, secondNode2);
            Assert.AreEqual(simpleConstructorsClassNode, simpleConstructorsClassNode2);
        }

        /// <summary>
        /// This is to test AvroSerializer.Create<AvroNode>() 
        /// Manually changing return type from object into strong type like AvroClassNode in AvroNode class will result in GetSchema() fail. 
        /// I have manually synced all the IList into List in auto generated code. Otherwise AvroSerializer.Create<AvroNode>() will throw the following error
        /// "Could not find any matching known type for 'System.Collections.Generic.IList`1[Org.Apache.REEF.Tang.Implementations.ClassHierarchy.AvroDataContract.AvroConstructorDef]'."
        /// </summary>
        [TestMethod]
        public void TestGetSchema()
        {
            var serializer = AvroSerializer.Create<AvroNode>();
            var s = serializer.WriterSchema.ToString();
            Assert.IsNotNull(s);
        }

        /// <summary>
        /// This test is to merge two AvroClassHierarchies
        /// </summary>
        [TestMethod]
        public void TestAvroClassHierarchyMerge()
        {
            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(
                new string[] { typeof(Timer).Assembly.GetName().Name });
            IClassNode timerClassNode = (IClassNode)ns.GetNode(typeof(Timer).AssemblyQualifiedName);

            AvroNode n = _serializer.ToAvroNode(ns);
            IClassHierarchy ns2 = _serializer.FromAvroNode(n);

            IClassHierarchy ns3 = TangFactory.GetTang().GetClassHierarchy(
                    new string[] { typeof(AvroNode).Assembly.GetName().Name });
            IClassNode avroNodeClassNode = (IClassNode)ns3.GetNode(typeof(AvroNode).AssemblyQualifiedName);

            AvroNode n2 = _serializer.ToAvroNode(ns3);
            IClassHierarchy ns4 = _serializer.FromAvroNode(n2);

            var ns5 = ns2.Merge(ns4);

            IClassNode timerClassNode2 = (IClassNode)ns5.GetNode(typeof(Timer).AssemblyQualifiedName);
            IClassNode avroNodeClassNode2 = (IClassNode)ns5.GetNode(typeof(AvroNode).AssemblyQualifiedName);

            Assert.AreEqual(timerClassNode, timerClassNode2);
            Assert.AreEqual(avroNodeClassNode, avroNodeClassNode2);
        }

        /// <summary>
        /// Test serialize a class hierarchy into a test file
        /// </summary>
        [TestMethod]
        public void TestToTextFileForTask()
        {
            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(new string[] { typeof(HelloTask).Assembly.GetName().Name });
            _serializer.ToTextFile(ns, "avroTask.bin");
            Assert.IsTrue(File.Exists("avroTask.bin"));
        }

        /// <summary>
        /// Test serialize a class hierarchy into a JSon string
        /// </summary>
        [TestMethod]
        public void TestToString()
        {
            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(new string[] { typeof(HelloTask).Assembly.GetName().Name });
            string s = _serializer.ToString(ns);
            Assert.IsNotNull(s);
        }

        /// <summary>
        /// Test serialize a class hierarchy to a Json string then deserialize it
        /// In deserialization, in ParseSubHierarchy(), exception is thrown: 
        /// Unable to cast object of type 'Newtonsoft.Json.Linq.JObject' to type 'Org.Apache.REEF.Tang.Implementations.ClassHierarchy.AvroDataContract.AvroClassNode'.
        /// This is because auto generated code use object as return type instead of AvroClassNode
        /// </summary>
        [Ignore]  // TODO: after Avro fix the issue. Enable the test
        [TestMethod]
        public void TestToFromJsonString()
        {
            Type timerType = typeof(Timer);
            Type secondType = typeof(Timer.Seconds);
            Type simpleConstructorType = typeof(SimpleConstructors);

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(
                new string[] { typeof(Timer).Assembly.GetName().Name, typeof(SimpleConstructors).Assembly.GetName().Name });
            IClassNode timerClassNode = (IClassNode)ns.GetNode(timerType.AssemblyQualifiedName);
            INode secondNode = ns.GetNode(secondType.AssemblyQualifiedName);
            IClassNode simpleConstructorsClassNode = (IClassNode)ns.GetNode(simpleConstructorType.AssemblyQualifiedName);

            string s = _serializer.ToString(ns);
            IClassHierarchy ns2 = _serializer.FromString(s);

            IClassNode timerClassNode2 = (IClassNode)ns2.GetNode(timerType.AssemblyQualifiedName);
            INode secondNode2 = ns2.GetNode(secondType.AssemblyQualifiedName);
            IClassNode simpleConstructorsClassNode2 = (IClassNode)ns2.GetNode(simpleConstructorType.AssemblyQualifiedName);

            Assert.AreEqual(timerClassNode, timerClassNode2);
            Assert.AreEqual(secondNode, secondNode2);
            Assert.AreEqual(simpleConstructorsClassNode, simpleConstructorsClassNode2);
        }

        /// <summary>
        /// Test serialize a class hierarchy to a text file then deserialize it
        /// In deserialization, in ParseSubHierarchy(), exception is thrown: 
        /// Unable to cast object of type 'Newtonsoft.Json.Linq.JObject' to type 'Org.Apache.REEF.Tang.Implementations.ClassHierarchy.AvroDataContract.AvroClassNode'.
        /// This is because auto generated code use object as return type instead of AvroClassNode
        /// </summary>
        [Ignore] // TODO: after Avro fix the issue. Enable the test
        [TestMethod]
        public void TestToFromTextFile()
        {
            Type timerType = typeof(Timer);
            Type secondType = typeof(Timer.Seconds);
            Type simpleConstructorType = typeof(SimpleConstructors);

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(
                new string[] { typeof(Timer).Assembly.GetName().Name, typeof(SimpleConstructors).Assembly.GetName().Name });
            IClassNode timerClassNode = (IClassNode)ns.GetNode(timerType.AssemblyQualifiedName);
            INode secondNode = ns.GetNode(secondType.AssemblyQualifiedName);
            IClassNode simpleConstructorsClassNode = (IClassNode)ns.GetNode(simpleConstructorType.AssemblyQualifiedName);

            _serializer.ToTextFile(ns, "avroEven.txt");
            IClassHierarchy ns2 = _serializer.FromTextFile("avroEven.txt");

            IClassNode timerClassNode2 = (IClassNode)ns2.GetNode(timerType.AssemblyQualifiedName);
            INode secondNode2 = ns2.GetNode(secondType.AssemblyQualifiedName);
            IClassNode simpleConstructorsClassNode2 = (IClassNode)ns2.GetNode(simpleConstructorType.AssemblyQualifiedName);

            Assert.AreEqual(timerClassNode, timerClassNode2);
            Assert.AreEqual(secondNode, secondNode2);
            Assert.AreEqual(simpleConstructorsClassNode, simpleConstructorsClassNode2);
        }

        /// <summary>
        /// Test serialize a class hierarchy to a file and deserialize from the file
        /// Currently, in ToFile() method, writer.Write(avroNodeData) throw exception "Value cannot be null.\r\nParameter name: value". 
        /// </summary>
        [Ignore]  // TODO: after Avro fix the issue. Enable the test
        [TestMethod]
        public void TestToFromFile()
        {
            Type timerType = typeof(Timer);
            Type secondType = typeof(Timer.Seconds);
            Type simpleConstructorType = typeof(SimpleConstructors);

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(
                new string[] { typeof(Timer).Assembly.GetName().Name, typeof(SimpleConstructors).Assembly.GetName().Name });
            IClassNode timerClassNode = (IClassNode)ns.GetNode(timerType.AssemblyQualifiedName);
            INode secondNode = ns.GetNode(secondType.AssemblyQualifiedName);
            IClassNode simpleConstructorsClassNode = (IClassNode)ns.GetNode(simpleConstructorType.AssemblyQualifiedName);

            _serializer.ToFile(ns, "avroEven.bin");
            IClassHierarchy ns2 = _serializer.FromFile("avroEven.bin");

            IClassNode timerClassNode2 = (IClassNode)ns2.GetNode(timerType.AssemblyQualifiedName);
            INode secondNode2 = ns2.GetNode(secondType.AssemblyQualifiedName);
            IClassNode simpleConstructorsClassNode2 = (IClassNode)ns2.GetNode(simpleConstructorType.AssemblyQualifiedName);

            Assert.AreEqual(timerClassNode, timerClassNode2);
            Assert.AreEqual(secondNode, secondNode2);
            Assert.AreEqual(simpleConstructorsClassNode, simpleConstructorsClassNode2);
        }

        /// <summary>
        /// Test serialize class hierarchy to byte array and deserializa back to class hierarchy
        /// AvroSerializer.Serialize(stream, obj) doesn't allow any null values in the obj to be serialized even if it is nullable
        /// </summary>
        [Ignore]  // TODO: after Avro fix the issue. Enable the test
        [TestMethod]
        public void TestToFromByteArray()
        {
            Type timerType = typeof(Timer);
            Type secondType = typeof(Timer.Seconds);
            Type simpleConstructorType = typeof(SimpleConstructors);

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(
                new string[] { typeof(Timer).Assembly.GetName().Name, typeof(SimpleConstructors).Assembly.GetName().Name });
            IClassNode timerClassNode = (IClassNode)ns.GetNode(timerType.AssemblyQualifiedName);
            INode secondNode = ns.GetNode(secondType.AssemblyQualifiedName);
            IClassNode simpleConstructorsClassNode = (IClassNode)ns.GetNode(simpleConstructorType.AssemblyQualifiedName);

            byte[] b = _serializer.ToByteArray(ns);
            IClassHierarchy ns2 = _serializer.FromByteArray(b);

            IClassNode timerClassNode2 = (IClassNode)ns2.GetNode(timerType.AssemblyQualifiedName);
            INode secondNode2 = ns2.GetNode(secondType.AssemblyQualifiedName);
            IClassNode simpleConstructorsClassNode2 = (IClassNode)ns2.GetNode(simpleConstructorType.AssemblyQualifiedName);

            Assert.AreEqual(timerClassNode, timerClassNode2);
            Assert.AreEqual(secondNode, secondNode2);
            Assert.AreEqual(simpleConstructorsClassNode, simpleConstructorsClassNode2);
        }
    }
}

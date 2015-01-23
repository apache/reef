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

using System.Globalization;
using System.IO;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Protobuf;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Reef.Tang.Examples;

namespace Org.Apache.Reef.Tang.Test.ClassHierarchy
{
    [TestClass]
    public class TestClassHierarchyRoundTrip : TestClassHierarchy
    {
        private void setup1()
        {
            TangImpl.Reset();
            ns = TangFactory.GetTang().GetClassHierarchy(new string[] { FileNames.Examples, FileNames.Common, FileNames.Tasks });
        }

        private void setup2()
        {
            TangImpl.Reset();
            ns = new ProtocolBufferClassHierarchy(ProtocolBufferClassHierarchy.Serialize(ns));
        }

        private void setup3()
        {
            TangImpl.Reset();
            try
            {
                ProtocolBufferClassHierarchy.Serialize("testProto.bin", ns);
                ns = ProtocolBufferClassHierarchy.DeSerialize("testProto.bin");
            }
            catch (IOException e)
            {
                Assert.Fail(string.Format(CultureInfo.CurrentCulture, "IOException when serialize/deserialize proto buffer file", e));
            }
        }

        //[TestMethod]
        //public new void TestString() 
        //{
        //    setup1();
        //    base.TestSimpleConstructors();
        //    setup2();
        //    base.TestSimpleConstructors();
        //    setup3();
        //    base.TestSimpleConstructors();
        //}
    }
}

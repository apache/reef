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
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Org.Apache.REEF.Tang.Examples;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Protobuf;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.ClassHierarchy
{
    public class TestAnonymousType
    {
        const string ClassHierarchyBinFileName = "example.bin";

        [Fact]
        public void TestAnonymousTypeWithDictionary()
        {
            List<string> appDlls = new List<string>();
            appDlls.Add(typeof(AnonymousType).GetTypeInfo().Assembly.GetName().Name);
            var c = TangFactory.GetTang().GetClassHierarchy(appDlls.ToArray());
            c.GetNode(typeof(AnonymousType).AssemblyQualifiedName);

            IConfiguration conf = TangFactory.GetTang().NewConfigurationBuilder(c).Build();
            IInjector injector = TangFactory.GetTang().NewInjector(conf);
            var obj = injector.GetInstance<AnonymousType>();
            Assert.NotNull(obj);

            var cd = Directory.GetCurrentDirectory();
            Console.WriteLine(cd);

            ProtocolBufferClassHierarchy.Serialize(ClassHierarchyBinFileName, c);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize(ClassHierarchyBinFileName);
            ch.GetNode(typeof(AnonymousType).AssemblyQualifiedName);
        }
    }
}

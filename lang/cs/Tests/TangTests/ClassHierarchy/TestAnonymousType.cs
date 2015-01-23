//<copyright>
//Copyright (c) Microsoft Corporation.  All rights reserved.
//</copyright>

using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Reef.Tang.Examples;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Protobuf;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Org.Apache.Reef.Tang.Test.ClassHierarchy
{
    [TestClass]
    public class TestAnonymousType
    {
        const string ClassHierarchyBinFileName = "example.bin";

        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            TangImpl.Reset();
        }

        [TestMethod]
        public void TestAnonymousTypeWithDictionary()
        {
            List<string> appDlls = new List<string>();
            appDlls.Add(typeof(AnonymousType).Assembly.GetName().Name);
            var c = TangFactory.GetTang().GetClassHierarchy(appDlls.ToArray());
            c.GetNode(typeof(AnonymousType).AssemblyQualifiedName);

            IConfiguration conf = TangFactory.GetTang().NewConfigurationBuilder(c).Build();
            IInjector injector = TangFactory.GetTang().NewInjector(conf);
            var obj = injector.GetInstance<AnonymousType>();
            Assert.IsNotNull(obj);

            var cd = Directory.GetCurrentDirectory();
            Console.WriteLine(cd);

            ProtocolBufferClassHierarchy.Serialize(ClassHierarchyBinFileName, c);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize(ClassHierarchyBinFileName);
            ch.GetNode(typeof(AnonymousType).AssemblyQualifiedName);
        }
    }
}

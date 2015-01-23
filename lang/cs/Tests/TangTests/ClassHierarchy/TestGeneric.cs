//<copyright>
//Copyright (c) Microsoft Corporation.  All rights reserved.
//</copyright>

using System.Collections.Generic;
using Org.Apache.Reef.Tang.Examples;
using Org.Apache.Reef.Tang.Implementations;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Reef.Wake.RX;

namespace Org.Apache.Reef.Tang.Test.ClassHierarchy
{
    [TestClass]
    public class TestGeneric
    {
        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            TangImpl.Reset();
        }

        [TestMethod]
        public void TestGenericClassWithT()
        {
            List<string> appDlls = new List<string>();
            appDlls.Add(typeof(GenericArgument<>).Assembly.GetName().Name);
            appDlls.Add(typeof(AbstractObserver<>).Assembly.GetName().Name);
            TangFactory.GetTang().GetClassHierarchy(appDlls.ToArray());
        }
    }  
}
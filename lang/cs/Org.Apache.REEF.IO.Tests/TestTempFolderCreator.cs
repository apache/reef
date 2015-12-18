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

using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.IO.TempFileCreation;
using Org.Apache.REEF.Tang.Implementations.Tang;

namespace Org.Apache.REEF.IO.Tests
{
    [TestClass]
    public class TestTempFolderCreator
    {
        /// <summary>
        /// This is to test default TempFileFolder and ITempFileCreator
        /// </summary>
        [TestMethod]
        public void TestDefaultTempFolder()
        {
            var b = TempFileConfigurationModule.ConfigurationModule.Build();
            var i = TangFactory.GetTang().NewInjector(b);
            var tempFileCreator = i.GetInstance<ITempFileCreator>();
            var f1 = tempFileCreator.TempFileFolder;
            var f2 = i.GetNamedInstance(typeof(TempFileFolder));
            Assert.IsTrue(f1.Equals(f2));
        }

        /// <summary>
        /// This is to test setting a value to TempFileFolder
        /// </summary>
        [TestMethod]
        public void TestTempFileFolerParameter()
        {
            var b = TempFileConfigurationModule.ConfigurationModule
                .Set(TempFileConfigurationModule.TempFileFolerParameter, @".\test1\abc\")
                .Build();
            var i = TangFactory.GetTang().NewInjector(b);
            var tempFileCreator = i.GetInstance<ITempFileCreator>();
            var f1 = tempFileCreator.TempFileFolder;
            var f2 = i.GetNamedInstance(typeof(TempFileFolder));
            Assert.IsTrue(f1.Equals(f2));
        }

        /// <summary>
        /// This is to test CreateTempDirectory() by providing a subfolder 
        /// </summary>
        [TestMethod]
        public void TestCreateTempFileFoler()
        {
            var b = TempFileConfigurationModule.ConfigurationModule
                .Set(TempFileConfigurationModule.TempFileFolerParameter, @"./test1/abc/")
                .Build();
            var i = TangFactory.GetTang().NewInjector(b);
            var tempFileCreator = i.GetInstance<ITempFileCreator>();
            var f1 = tempFileCreator.CreateTempDirectory("ddd\\fff");
            var f3 = tempFileCreator.CreateTempDirectory("ddd\\fff", "bbb");
            var f2 = Path.GetFullPath(@"./test1/abc/" + "ddd\\fff");
            Assert.IsTrue(f1.StartsWith(f2));
            Assert.IsTrue(f3.EndsWith("bbb"));
        }
    }
}

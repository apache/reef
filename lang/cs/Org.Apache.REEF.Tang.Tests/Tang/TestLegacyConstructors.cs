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

using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Tests.Tang
{
    [TestClass]
    public class TestLegacyConstructors
    {   
        static ITang tang;

        [TestInitialize()]
        public void TestSetup()
        {
            tang = TangFactory.GetTang();
        }

        [TestMethod]
        public void TestLegacyConstructor()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();

            IList<string> constructorArg = new List<string>();
            constructorArg.Add(ReflectionUtilities.GetAssemblyQualifiedName(typeof(int)));
            constructorArg.Add(ReflectionUtilities.GetAssemblyQualifiedName(typeof(string)));
            cb.RegisterLegacyConstructor(ReflectionUtilities.GetAssemblyQualifiedName(typeof(LegacyConstructor)), constructorArg);
            // cb.Bind(typeof(LegacyConstructor), typeof(LegacyConstructor));
            cb.BindImplementation(GenericType<LegacyConstructor>.Class, GenericType<LegacyConstructor>.Class);

            IInjector i = tang.NewInjector(cb.Build());
            i.BindVolatileInstance(GenericType<int>.Class, 42);
            i.BindVolatileInstance(GenericType<string>.Class, "The meaning of life is ");
            LegacyConstructor l = i.GetInstance<LegacyConstructor>();
            Assert.AreEqual(42, l.X);
            Assert.AreEqual("The meaning of life is ", l.Y);
        }
    }

    class LegacyConstructor
    {
        public LegacyConstructor(int x, string y)
        {
            this.X = x;
            this.Y = y;
        }
        
        public int X { get; set; }
        
        public string Y { get; set; }
    }
}
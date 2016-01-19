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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.ScenarioTest
{
    public class TestDefaultConstructor
    {
        [Fact]
        public void TestDefaultConstructorWithoutBinding()
        {
            var r = (A)TangFactory.GetTang().NewInjector().GetInstance(typeof(A));
            System.Diagnostics.Debug.WriteLine(r.Instance);
        }
    }

    class A
    {
        [Inject]
        public A()
        {
            Instance = "default";
        }
        [Inject]
        public A(B b)
        {
            Instance = "non default";
        }
    
        public string Instance { get; set; }
    }

    class B
    {
        [Inject]
        public B()
        {
            Instance = "default";
        }
        [Inject]
        public B(C c)
        {
            Instance = "non default";
        }

        public string Instance { get; set; }
    }

    class C
    {
        [Inject]
        public C()
        {
            Instance = "default";
        }
    
        public string Instance { get; set; }
    }
}
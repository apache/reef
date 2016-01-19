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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.ClassHierarchy
{
    public class TestMultipleInterface
    {
        [Fact]
        public void TestFoo()
        {
            var ch = TangFactory.GetTang().GetDefaultClassHierarchy();
            var fNode = ch.GetNode(typeof(Org.Apache.REEF.Tang.Tests.ClassHierarchy.Foo));
            var bNode = ch.GetNode(typeof(Org.Apache.REEF.Tang.Tests.ClassHierarchy.Bar));
            var fbNode = ch.GetNode(typeof(Org.Apache.REEF.Tang.Tests.ClassHierarchy.BarFoo));
        }
    }

    public class Foo : IObserver<int>
    {
        [Inject]
        public Foo()
        {
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnNext(int value)
        {
            throw new NotImplementedException();
        }
    }

    public class Bar : IObserver<string>
    {
        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnNext(string value)
        {
            throw new NotImplementedException();
        }
    }

    public class BarFoo : IObserver<string>, IObserver<int>
    {
        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnNext(string value)
        {
            throw new NotImplementedException();
        }

        public void OnNext(int value)
        {
            throw new NotImplementedException();
        }
    }
}
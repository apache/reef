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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.Tang
{
    public class TestExternalConstructors
    {
        static ITang tang;

        public TestExternalConstructors()
        {
            tang = TangFactory.GetTang();
        }

        [Fact]
        public void TestBindConstructor()
        {
            ICsConfigurationBuilder b = TangFactory.GetTang().NewConfigurationBuilder();
            b.BindConstructor(GenericType<A>.Class, GenericType<ACons>.Class);
            b.BindConstructor(GenericType<B>.Class, GenericType<BCons>.Class);

            TangFactory.GetTang().NewInjector(b.Build()).GetInstance(typeof(B));
        }

        [Fact]
        public void TestSImpleExternalConstructor()
        {
            ICsConfigurationBuilder b = TangFactory.GetTang().NewConfigurationBuilder();
            b.BindConstructor(GenericType<A>.Class, GenericType<ACons>.Class);
            A aRef = (A)TangFactory.GetTang().NewInjector(b.Build()).GetInstance(typeof(A));
            Assert.NotNull(aRef);
        }

        [Fact]
        public void TestExternalLegacyConstructor()
        {
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder();
            cb.BindConstructor(GenericType<ExternalConstructorExample.Legacy>.Class, GenericType<ExternalConstructorExample.LegacyWrapper>.Class);
            IInjector i = tang.NewInjector(cb.Build());
            i.BindVolatileInstance(GenericType<int>.Class, 42);
            i.BindVolatileInstance(GenericType<string>.Class, "The meaning of life is ");
            ExternalConstructorExample.Legacy l = i.GetInstance<ExternalConstructorExample.Legacy>();
            Assert.Equal(42, l.X);
            Assert.Equal("The meaning of life is ", l.Y);
        }

        public class A
        {
            public A()
            {
            }
        }

        public class B
        {
            public B(A a)
            {
            }
        }

        public class ACons : IExternalConstructor<A>
        {
            [Inject]
            ACons()
            {
            }

            public A NewInstance()
            {
                return new A();
            }
        }

        public class BCons : IExternalConstructor<B>
        {
            private readonly A a;
            [Inject]
            BCons(A a)
            {
                this.a = a;
            }

            public B NewInstance()
            {
                return new B(a);
            }
        }
    }

    class ExternalConstructorExample
    {
        public class Legacy
        {
            public Legacy(int x, string y)
            {
                this.X = x;
                this.Y = y;
            }
        
            public int X { get; set; }

            public string Y { get; set; }
        }

        public class LegacyWrapper : IExternalConstructor<Legacy>
        {
            [Inject]
            LegacyWrapper(int x, string y)
            {
                this.X = x;
                this.Y = y;
            }

            public int X { get; set; }

            public string Y { get; set; }

            public Legacy NewInstance()
            {
                return new Legacy(X, Y);
            }
        }
    }
}
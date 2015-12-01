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
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.Injection
{
    interface IAinj
    {
    }

    [DefaultImplementation(typeof(C), "C")]
    interface IBinj : IAinj
    {
    }

    public class TestInjectionFuture
    {
        [Fact]
        public void TestProactiveFutures()
        {
            IInjector i = TangFactory.GetTang().NewInjector();
            IsFuture.Instantiated = false;
            i.GetInstance(typeof(NeedsFuture));
            Assert.True(IsFuture.Instantiated);
        }

        [Fact]
        public void testFutures() 
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IInjector i2 = TangFactory.GetTang().NewInjector(cb.Build());
    
            Futurist f = (Futurist)i.GetInstance(typeof(Futurist));
            Assert.True(f == f.getMyCar().getDriver());
            Assert.True(f.getMyCar() == f.getMyCar().getDriver().getMyCar());
    
            Futurist f2 = (Futurist)i2.GetInstance(typeof(Futurist));
            Assert.True(f2 == f2.getMyCar().getDriver());
            Assert.True(f2.getMyCar() == f2.getMyCar().getDriver().getMyCar());

            Assert.True(f != f2.getMyCar().getDriver());
            Assert.True(f.getMyCar() != f2.getMyCar().getDriver().getMyCar());
        }
        
        [Fact]
          public void testFutures2()  
          {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IInjector i2 = i.ForkInjector(new IConfiguration[] { });
    
            FlyingCar c = (FlyingCar)i.GetInstance(typeof(FlyingCar));
            Assert.True(c == c.getDriver().getMyCar());
            Assert.True(c.getDriver() == c.getDriver().getMyCar().getDriver());

            FlyingCar c2 = (FlyingCar)i2.GetInstance(typeof(FlyingCar));
            Assert.True(c2 == c2.getDriver().getMyCar());
            Assert.True(c2.getDriver() == c2.getDriver().getMyCar().getDriver());

            Assert.True(c2 != c.getDriver().getMyCar());
            Assert.True(c2.getDriver() != c.getDriver().getMyCar().getDriver());
          }

        [Fact]
          public void TestNamedParameterInjectionFuture() 
          {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation(GenericType<FlyingCar>.Class, GenericType<FlyingCar>.Class);
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            PickyFuturist f = (PickyFuturist)i.GetInstance(typeof(PickyFuturist));
            Assert.NotNull(f.getMyCar());
          }

         [Fact]
          public void TestNamedParameterInjectionFutureDefaultImpl() 
          {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            PickyFuturist f = (PickyFuturist)i.GetInstance(typeof(PickyFuturist));
            Assert.NotNull(f.getMyCar());
          }

        [Fact]
          public void TestNamedParameterInjectionFutureBindImpl()
          {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation(GenericType<Futurist>.Class, GenericType<PickyFuturist>.Class);
            cb.BindNamedParameter<MyFlyingCar, BigFlyingCar, FlyingCar>(GenericType<MyFlyingCar>.Class, GenericType<BigFlyingCar>.Class);
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            PickyFuturist f = (PickyFuturist)i.GetInstance(typeof(PickyFuturist));
            Assert.NotNull((BigFlyingCar)f.getMyCar());
          }

        [Fact]
        public void TestNamedParameterBoundToDelegatingInterface() 
        {
            IInjector i = TangFactory.GetTang().NewInjector();
            C c = (C)i.GetNamedInstance(typeof(AName));
            Assert.NotNull(c);
        }

        [Fact]
        public void TestBoundToDelegatingInterface() 
        {
            IInjector i = TangFactory.GetTang().NewInjector();
            C c = (C)i.GetInstance(typeof(IBinj));
            Assert.NotNull(c);
        }

        [DefaultImplementation(typeof(Futurist), "Futurist")]
        public class Futurist 
        {
            private readonly IInjectionFuture<FlyingCar> fcar;
            [Inject]
            public Futurist(IInjectionFuture<FlyingCar> car) 
            {
                this.fcar = car;
            }

            public virtual FlyingCar getMyCar() 
            {
                FlyingCar c = fcar.Get();
                return c;
            }    
        }
  
        public class PickyFuturist : Futurist 
        {
            private readonly IInjectionFuture<FlyingCar> fCar;
            [Inject]
            public PickyFuturist([Parameter(typeof(MyFlyingCar))] IInjectionFuture<FlyingCar> myFlyingCar) : base(myFlyingCar)
            {
                fCar = myFlyingCar;
            }

            public override FlyingCar getMyCar() 
            {
                FlyingCar c = fCar.Get();
                return c;
            }    
        }

        [DefaultImplementation(typeof(FlyingCar), "")]
        public class FlyingCar 
        {
            private readonly string color;
            private readonly Futurist driver;
            
            [Inject]
            public FlyingCar([Parameter(typeof(Color))] string color, Futurist driver) 
            {
                this.color = color;
                this.driver = driver;
            }
    
            public string getColor() 
            {
                return color;
            }
    
            public Futurist getDriver() 
            {
                return driver;
            }
        }

        [NamedParameter(DefaultValue = "blue")]
        public class Color : Name<string>
        {
        }

        public class BigFlyingCar : FlyingCar 
        {
            [Inject]
            BigFlyingCar([Parameter(typeof(Color))] string color, Futurist driver) : base(color, driver)
            {
            }
        }
  
        [NamedParameter(DefaultClass = typeof(FlyingCar))]
        public class MyFlyingCar : Name<FlyingCar>
        {            
        }
    }

    [NamedParameter(DefaultClass = typeof(IBinj))]
    class AName : Name<IAinj>
    {        
    }

    class C : IBinj
    {
        [Inject]
        C()
        {            
        }
    }

    class IsFuture
    {
        [Inject]
        IsFuture(NeedsFuture nf)
        {
            Instantiated = true;
        }
    
        public static bool Instantiated { get; set; }    
    }

    class NeedsFuture
    {
        [Inject]
        NeedsFuture(IInjectionFuture<IsFuture> isFut)
        {
        }
    }
}
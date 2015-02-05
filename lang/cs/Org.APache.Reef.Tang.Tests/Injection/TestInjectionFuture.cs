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

using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Org.Apache.Reef.Tang.Test.Injection
{
    interface IAinj
    {
    }

    [DefaultImplementation(typeof(C), "C")]
    interface IBinj : IAinj
    {
    }

    [TestClass]
    public class TestInjectionFuture
    {
        [TestMethod]
        public void TestProactiveFutures()
        {
            IInjector i = TangFactory.GetTang().NewInjector();
            IsFuture.Instantiated = false;
            i.GetInstance(typeof(NeedsFuture));
            Assert.IsTrue(IsFuture.Instantiated);
        }

        [TestMethod]
        public void testFutures() 
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IInjector i2 = TangFactory.GetTang().NewInjector(cb.Build());
    
            Futurist f = (Futurist)i.GetInstance(typeof(Futurist));
            Assert.IsTrue(f == f.getMyCar().getDriver());
            Assert.IsTrue(f.getMyCar() == f.getMyCar().getDriver().getMyCar());
    
            Futurist f2 = (Futurist)i2.GetInstance(typeof(Futurist));
            Assert.IsTrue(f2 == f2.getMyCar().getDriver());
            Assert.IsTrue(f2.getMyCar() == f2.getMyCar().getDriver().getMyCar());

            Assert.IsTrue(f != f2.getMyCar().getDriver());
            Assert.IsTrue(f.getMyCar() != f2.getMyCar().getDriver().getMyCar());
        }
        
        [TestMethod]
          public void testFutures2()  
          {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            IInjector i2 = i.ForkInjector(new IConfiguration[] { });
    
            FlyingCar c = (FlyingCar)i.GetInstance(typeof(FlyingCar));
            Assert.IsTrue(c == c.getDriver().getMyCar());
            Assert.IsTrue(c.getDriver() == c.getDriver().getMyCar().getDriver());

            FlyingCar c2 = (FlyingCar)i2.GetInstance(typeof(FlyingCar));
            Assert.IsTrue(c2 == c2.getDriver().getMyCar());
            Assert.IsTrue(c2.getDriver() == c2.getDriver().getMyCar().getDriver());

            Assert.IsTrue(c2 != c.getDriver().getMyCar());
            Assert.IsTrue(c2.getDriver() != c.getDriver().getMyCar().getDriver());
          }

        [TestMethod]
          public void TestNamedParameterInjectionFuture() 
          {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation(GenericType<FlyingCar>.Class, GenericType<FlyingCar>.Class);
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            PickyFuturist f = (PickyFuturist)i.GetInstance(typeof(PickyFuturist));
            Assert.IsNotNull(f.getMyCar());
          }

         [TestMethod]
          public void TestNamedParameterInjectionFutureDefaultImpl() 
          {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            PickyFuturist f = (PickyFuturist)i.GetInstance(typeof(PickyFuturist));
            Assert.IsNotNull(f.getMyCar());
          }

        [TestMethod]
          public void TestNamedParameterInjectionFutureBindImpl()
          {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation(GenericType<Futurist>.Class, GenericType<PickyFuturist>.Class);
            cb.BindNamedParameter<MyFlyingCar, BigFlyingCar, FlyingCar>(GenericType<MyFlyingCar>.Class, GenericType<BigFlyingCar>.Class);
            IInjector i = TangFactory.GetTang().NewInjector(cb.Build());
            PickyFuturist f = (PickyFuturist)i.GetInstance(typeof(PickyFuturist));
            Assert.IsNotNull((BigFlyingCar)f.getMyCar());
          }

        [TestMethod]
        public void TestNamedParameterBoundToDelegatingInterface() 
        {
            IInjector i = TangFactory.GetTang().NewInjector();
            C c = (C)i.GetNamedInstance(typeof(AName));
            Assert.IsNotNull(c);
        }

        [TestMethod]
        public void TestBoundToDelegatingInterface() 
        {
            IInjector i = TangFactory.GetTang().NewInjector();
            C c = (C)i.GetInstance(typeof(IBinj));
            Assert.IsNotNull(c);
        }

        [DefaultImplementation(typeof(Futurist), "Futurist")]
        public class Futurist 
        {
            private IInjectionFuture<FlyingCar> fcar;
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
            private IInjectionFuture<FlyingCar> fCar;
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
            private string color;
            private Futurist driver;
            
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
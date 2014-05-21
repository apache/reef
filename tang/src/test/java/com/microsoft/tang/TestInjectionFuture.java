/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.tang;

import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;

public class TestInjectionFuture {
  @Test
  public void testFutures() throws InjectionException, BindException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    Injector i2 = Tang.Factory.getTang().newInjector(cb.build());

    Futurist f = i.getInstance(Futurist.class);
    Assert.assertTrue(f == f.getMyCar().getDriver());
    Assert.assertTrue(f.getMyCar() == f.getMyCar().getDriver().getMyCar());

    Futurist f2 = i2.getInstance(Futurist.class);
    Assert.assertTrue(f2 == f2.getMyCar().getDriver());
    Assert.assertTrue(f2.getMyCar() == f2.getMyCar().getDriver().getMyCar());

    Assert.assertTrue(f != f2.getMyCar().getDriver());
    Assert.assertTrue(f.getMyCar() != f2.getMyCar().getDriver().getMyCar());

  }

  @Test
  public void testFutures2() throws InjectionException, BindException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    Injector i2 = i.forkInjector();

    FlyingCar c = i.getInstance(FlyingCar.class);
    Assert.assertTrue(c == c.getDriver().getMyCar());
    Assert.assertTrue(c.getDriver() == c.getDriver().getMyCar().getDriver());

    FlyingCar c2 = i2.getInstance(FlyingCar.class);
    Assert.assertTrue(c2 == c2.getDriver().getMyCar());
    Assert.assertTrue(c2.getDriver() == c2.getDriver().getMyCar().getDriver());

    Assert.assertTrue(c2 != c.getDriver().getMyCar());
    Assert.assertTrue(c2.getDriver() != c.getDriver().getMyCar().getDriver());

  }

  @Test
  public void testNamedParameterInjectionFuture() throws InjectionException, BindException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(FlyingCar.class, FlyingCar.class);
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    PickyFuturist f = i.getInstance(PickyFuturist.class);
    Assert.assertNotNull(f.getMyCar());
  }

  @Test
  public void testNamedParameterInjectionFutureDefaultImpl() throws InjectionException, BindException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    PickyFuturist f = i.getInstance(PickyFuturist.class);
    Assert.assertNotNull(f.getMyCar());
  }

  @Test
  public void testNamedParameterInjectionFutureBindImpl() throws InjectionException, BindException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(Futurist.class, PickyFuturist.class);
    cb.bindNamedParameter(MyFlyingCar.class, BigFlyingCar.class);
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    PickyFuturist f = i.getInstance(PickyFuturist.class);
    Assert.assertNotNull((BigFlyingCar) f.getMyCar());
  }

  @Test
  public void testNamedParameterBoundToDelegatingInterface() throws InjectionException, BindException {
    Injector i = Tang.Factory.getTang().newInjector();
    @SuppressWarnings("unused")
    C c = (C) i.getNamedInstance(AName.class);
  }

  @Test
  public void testBoundToDelegatingInterface() throws InjectionException, BindException {
    Injector i = Tang.Factory.getTang().newInjector();
    @SuppressWarnings("unused")
    C c = (C) i.getInstance(B.class);
  }


  @DefaultImplementation(Futurist.class)
  public static class Futurist {
    private final InjectionFuture<FlyingCar> f_car;

    @Inject
    public Futurist(InjectionFuture<FlyingCar> car) {
      this.f_car = car;
    }

    public FlyingCar getMyCar() {
      FlyingCar c = f_car.get();
      return c;
    }
  }

  public static class PickyFuturist extends Futurist {
    private final InjectionFuture<FlyingCar> f_car;

    @Inject
    public PickyFuturist(@Parameter(MyFlyingCar.class) InjectionFuture<FlyingCar> myFlyingCar) {
      super(myFlyingCar);
      f_car = myFlyingCar;
    }

    public FlyingCar getMyCar() {
      FlyingCar c = f_car.get();
      return c;
    }
  }

  @DefaultImplementation(FlyingCar.class)
  public static class FlyingCar {
    private final String color;
    private final Futurist driver;

    @NamedParameter(default_value = "blue")
    class Color implements Name<String> {
    }

    @Inject
    FlyingCar(@Parameter(Color.class) String color, Futurist driver) {
      this.color = color;
      this.driver = driver;
    }

    public String getColor() {
      return color;
    }

    public Futurist getDriver() {
      return driver;
    }
  }

  public static class BigFlyingCar extends FlyingCar {
    @Inject
    BigFlyingCar(@Parameter(Color.class) String color, Futurist driver) {
      super(color, driver);
    }
  }

  @NamedParameter(default_class = FlyingCar.class)
  public static class MyFlyingCar implements Name<FlyingCar> {
  }

}

@NamedParameter(default_class = B.class)
class AName implements Name<A> {
}

interface A {
}

@DefaultImplementation(C.class)
interface B extends A {
}

class C implements B {
  @Inject
  C() {
  }
}

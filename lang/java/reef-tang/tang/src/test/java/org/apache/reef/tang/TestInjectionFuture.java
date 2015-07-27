/*
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
package org.apache.reef.tang;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;

interface A {
}

@DefaultImplementation(C.class)
interface B extends A {
}

public class TestInjectionFuture {
  @Test
  public void testFutures() throws InjectionException, BindException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector i = Tang.Factory.getTang().newInjector(cb.build());
    final Injector i2 = Tang.Factory.getTang().newInjector(cb.build());

    final Futurist f = i.getInstance(Futurist.class);
    Assert.assertTrue(f == f.getMyCar().getDriver());
    Assert.assertTrue(f.getMyCar() == f.getMyCar().getDriver().getMyCar());

    final Futurist f2 = i2.getInstance(Futurist.class);
    Assert.assertTrue(f2 == f2.getMyCar().getDriver());
    Assert.assertTrue(f2.getMyCar() == f2.getMyCar().getDriver().getMyCar());

    Assert.assertTrue(f != f2.getMyCar().getDriver());
    Assert.assertTrue(f.getMyCar() != f2.getMyCar().getDriver().getMyCar());

  }

  @Test
  public void testFutures2() throws InjectionException, BindException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector i = Tang.Factory.getTang().newInjector(cb.build());
    final Injector i2 = i.forkInjector();

    final FlyingCar c = i.getInstance(FlyingCar.class);
    Assert.assertTrue(c == c.getDriver().getMyCar());
    Assert.assertTrue(c.getDriver() == c.getDriver().getMyCar().getDriver());

    final FlyingCar c2 = i2.getInstance(FlyingCar.class);
    Assert.assertTrue(c2 == c2.getDriver().getMyCar());
    Assert.assertTrue(c2.getDriver() == c2.getDriver().getMyCar().getDriver());

    Assert.assertTrue(c2 != c.getDriver().getMyCar());
    Assert.assertTrue(c2.getDriver() != c.getDriver().getMyCar().getDriver());

  }

  @Test
  public void testNamedParameterInjectionFuture() throws InjectionException, BindException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(FlyingCar.class, FlyingCar.class);
    final Injector i = Tang.Factory.getTang().newInjector(cb.build());
    final PickyFuturist f = i.getInstance(PickyFuturist.class);
    Assert.assertNotNull(f.getMyCar());
  }

  @Test
  public void testNamedParameterInjectionFutureDefaultImpl() throws InjectionException, BindException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector i = Tang.Factory.getTang().newInjector(cb.build());
    final PickyFuturist f = i.getInstance(PickyFuturist.class);
    Assert.assertNotNull(f.getMyCar());
  }

  @Test
  public void testNamedParameterInjectionFutureBindImpl() throws InjectionException, BindException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(Futurist.class, PickyFuturist.class);
    cb.bindNamedParameter(MyFlyingCar.class, BigFlyingCar.class);
    final Injector i = Tang.Factory.getTang().newInjector(cb.build());
    final PickyFuturist f = i.getInstance(PickyFuturist.class);
    Assert.assertNotNull((BigFlyingCar) f.getMyCar());
  }

  @Test
  public void testNamedParameterBoundToDelegatingInterface() throws InjectionException, BindException {
    final Injector i = Tang.Factory.getTang().newInjector();
    @SuppressWarnings("unused") final C c = (C) i.getNamedInstance(AName.class);
  }

  @Test
  public void testBoundToDelegatingInterface() throws InjectionException, BindException {
    final Injector i = Tang.Factory.getTang().newInjector();
    @SuppressWarnings("unused") final C c = (C) i.getInstance(B.class);
  }


  @DefaultImplementation(Futurist.class)
  public static class Futurist {
    private final InjectionFuture<FlyingCar> fCar;

    @Inject
    public Futurist(final InjectionFuture<FlyingCar> car) {
      this.fCar = car;
    }

    public FlyingCar getMyCar() {
      final FlyingCar c = fCar.get();
      return c;
    }
  }

  public static class PickyFuturist extends Futurist {
    private final InjectionFuture<FlyingCar> fCar;

    @Inject
    public PickyFuturist(@Parameter(MyFlyingCar.class) final InjectionFuture<FlyingCar> myFlyingCar) {
      super(myFlyingCar);
      fCar = myFlyingCar;
    }

    public FlyingCar getMyCar() {
      final FlyingCar c = fCar.get();
      return c;
    }
  }

  @DefaultImplementation(FlyingCar.class)
  public static class FlyingCar {
    private final String color;
    private final Futurist driver;

    @Inject
    FlyingCar(@Parameter(Color.class) final String color, final Futurist driver) {
      this.color = color;
      this.driver = driver;
    }

    public String getColor() {
      return color;
    }

    public Futurist getDriver() {
      return driver;
    }

    @NamedParameter(default_value = "blue")
    class Color implements Name<String> {
    }
  }

  public static class BigFlyingCar extends FlyingCar {
    @Inject
    BigFlyingCar(@Parameter(Color.class) final String color, final Futurist driver) {
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

class C implements B {
  @Inject
  C() {
  }
}

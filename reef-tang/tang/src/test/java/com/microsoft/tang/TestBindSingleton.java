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

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationFile;
import org.junit.Assert;

import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;

import static org.junit.Assert.assertTrue;

public class TestBindSingleton {

  @Before
  public void before() {
    InbredSingletons.A.count = 0;
    InbredSingletons.B.count = 0;
    InbredSingletons.C.count = 0;

    IncestuousSingletons.A.count = 0;
    IncestuousSingletons.B.count = 0;
    IncestuousSingletons.BN.count = 0;
    IncestuousSingletons.C.count = 0;

    IncestuousInterfaceSingletons.A.count = 0;
    IncestuousInterfaceSingletons.B.count = 0;
    IncestuousInterfaceSingletons.BN.count = 0;
    IncestuousInterfaceSingletons.C.count = 0;
  }

  public static class A {
    @Inject
    public A() {
      // Intentionally blank
    }

  }

  public static class AA {
    @Inject
    public AA() {
      // Intentionally blank
    }

  }

  public static class B extends A {
    @Inject
    public B() {
      // intentionally blank
    }
  }

  @Test
  public void testSingletonRoundTrip() throws BindException, InjectionException {

    final JavaConfigurationBuilder b = Tang.Factory.getTang()
        .newConfigurationBuilder();
    b.bindImplementation(A.class, B.class);
    final Configuration src = b.build();

    final JavaConfigurationBuilder dest = Tang.Factory.getTang()
        .newConfigurationBuilder();
    ConfigurationFile.addConfiguration(dest, ConfigurationFile.toConfigurationString(src));
    final Injector i = Tang.Factory.getTang().newInjector(dest.build());
    final A a1 = i.getInstance(A.class);
    final A a2 = i.getInstance(A.class);
    final B b1 = i.getInstance(B.class);

    assertTrue("Two singletons should be the same", a1 == a2);
    assertTrue("Both instances should be of class B", a1 instanceof B);
    assertTrue("Both instances should be of class B", a2 instanceof B);
    assertTrue("Singleton and not singleton should be the same", a1 == b1);

    final Injector injector2 = Tang.Factory.getTang().newInjector(src);
    final A a3 = injector2.getInstance(A.class);
    assertTrue(
        "Two different injectors should return two different singletons",
        a3 != a1);

    final Injector injector3 = injector2.forkInjector();
    final A a4 = injector3.getInstance(A.class);
    assertTrue(
        "Child Injectors should return the same singletons as their parents",
        a3 == a4);
  }

  @Test
  public void testLateBoundVolatileInstanceWithSingletonX()
      throws BindException, InjectionException {
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindImplementation(LateBoundVolatile.A.class,
        LateBoundVolatile.B.class);
    final Injector i = tang.newInjector(cb.build());
    i.bindVolatileInstance(LateBoundVolatile.C.class, new LateBoundVolatile.C());
    i.getInstance(LateBoundVolatile.A.class);
  }

  @Test
  public void testMultipleInjectorInstaceWithSingleton() throws BindException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();

    final Injector i1 = Tang.Factory.getTang().newInjector(cb.build());
    final Injector i2 = Tang.Factory.getTang().newInjector(cb.build());

    assertTrue("Different injectors should return different singleton object instances", i1.getInstance(AA.class) != i2.getInstance(AA.class));

    final Configuration c = cb.build();

    final Injector i3 = Tang.Factory.getTang().newInjector(c);
    final Injector i4 = Tang.Factory.getTang().newInjector(c);

    assertTrue("Different injectors should return different singleton object instances", i3.getInstance(AA.class) != i4.getInstance(AA.class));

  }

  @Test
  public void testLateBoundVolatileInstanceWithSingletonY()
      throws BindException, InjectionException {
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    Injector i = tang.newInjector(cb.build());
    i.bindVolatileInstance(LateBoundVolatile.C.class, new LateBoundVolatile.C());
    i.getInstance(LateBoundVolatile.C.class);
  }

  @Test
  public void testLateBoundVolatileInstanceWithSingletonZ()
      throws BindException, InjectionException {
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindImplementation(LateBoundVolatile.B.class,
        LateBoundVolatile.B.class);
    Injector i = tang.newInjector(cb.build());
    i.bindVolatileInstance(LateBoundVolatile.C.class, new LateBoundVolatile.C());
    i.getInstance(LateBoundVolatile.B.class);
  }

  @Test
  public void testInbredSingletons() throws BindException, InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindImplementation(InbredSingletons.A.class,
        InbredSingletons.A.class);
    b.bindImplementation(InbredSingletons.B.class,
        InbredSingletons.B.class);
    b.bindImplementation(InbredSingletons.C.class,
        InbredSingletons.C.class);
    Injector i = t.newInjector(b.build());
    i.getInstance(InbredSingletons.A.class);
  }

  @Test
  public void testIncestuousSingletons() throws BindException,
      InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindImplementation(IncestuousSingletons.A.class,
        IncestuousSingletons.A.class);
    b.bindImplementation(IncestuousSingletons.B.class,
        IncestuousSingletons.B.class);
    b.bindImplementation(IncestuousSingletons.C.class,
        IncestuousSingletons.C.class);
    Injector i = t.newInjector(b.build());
    i.getInstance(IncestuousSingletons.A.class);
  }

  @Test
  public void testIncestuousSingletons2() throws BindException,
      InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindImplementation(IncestuousSingletons.A.class,
        IncestuousSingletons.A.class);
    b.bindImplementation(IncestuousSingletons.B.class,
        IncestuousSingletons.BN.class);
    b.bindImplementation(IncestuousSingletons.C.class,
        IncestuousSingletons.C.class);
    Injector i = t.newInjector(b.build());
    i.getInstance(IncestuousSingletons.A.class);
  }

  @Test
  public void testIncestuousInterfaceSingletons() throws BindException,
      InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindImplementation(IncestuousInterfaceSingletons.AI.class,
        IncestuousInterfaceSingletons.A.class);
    b.bindImplementation(IncestuousInterfaceSingletons.BI.class,
        IncestuousInterfaceSingletons.BN.class);
    b.bindImplementation(IncestuousInterfaceSingletons.CI.class,
        IncestuousInterfaceSingletons.C.class);
    Injector i = t.newInjector(b.build());
    i.getInstance(IncestuousInterfaceSingletons.AI.class);
  }

  @Test
  public void testIncestuousInterfaceSingletons2() throws BindException,
      InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindImplementation(IncestuousInterfaceSingletons.AI.class,
        IncestuousInterfaceSingletons.A.class);
    b.bindImplementation(IncestuousInterfaceSingletons.BI.class,
        IncestuousInterfaceSingletons.B.class);
    // TODO: Should we require bind(A,B), then bind(B,B) if B has subclasses?
    b.bindImplementation(IncestuousInterfaceSingletons.B.class,
        IncestuousInterfaceSingletons.B.class);
    b.bindImplementation(IncestuousInterfaceSingletons.CI.class,
        IncestuousInterfaceSingletons.C.class);
    Injector i = t.newInjector(b.build());
    i.getInstance(IncestuousInterfaceSingletons.AI.class);
  }

  @Test
  public void testIsBrokenClassInjectable() throws BindException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bind(IsBrokenClassInjectable.class, IsBrokenClassInjectable.class);
    Assert.assertTrue(t.newInjector(b.build()).isInjectable(
        IsBrokenClassInjectable.class));
  }

  @Test
  public void testIsBrokenSingletonClassInjectable() throws BindException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindImplementation(IsBrokenClassInjectable.class,
        IsBrokenClassInjectable.class);
    Assert.assertTrue(t.newInjector(b.build()).isInjectable(
        IsBrokenClassInjectable.class));
  }

  @Test(expected = InjectionException.class)
  public void testBrokenSingletonClassCantInject() throws BindException, InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindImplementation(IsBrokenClassInjectable.class,
        IsBrokenClassInjectable.class);
    Assert.assertTrue(t.newInjector(b.build()).isInjectable(
        IsBrokenClassInjectable.class));
    t.newInjector(b.build()).getInstance(IsBrokenClassInjectable.class);
  }
}

class LateBoundVolatile {
  static class A {
  }

  static class B extends A {
    @Inject
    B(C c) {
    }
  }

  static class C {
  }
}

class InbredSingletons {
  static class A {
    static int count = 0;

    @Inject
    A(B b) {
      Assert.assertEquals(0, count);
      count++;
    }
  }

  static class B {
    static int count = 0;

    @Inject
    B(C c) {
      Assert.assertEquals(0, count);
      count++;
    }
  }

  static class C {
    static int count = 0;

    @Inject
    C() {
      Assert.assertEquals(0, count);
      count++;
    }
  }
}

class IncestuousSingletons {
  static class A {
    static int count = 0;

    @Inject
    A(C c, B b) {
      Assert.assertEquals(0, count);
      count++;
    }
  }

  static class B {
    static int count = 0;

    protected B() {
    }

    @Inject
    B(C c) {
      Assert.assertEquals(0, count);
      count++;
    }
  }

  static class BN extends B {
    static int count = 0;

    @Inject
    BN(C c) {
      super();
      Assert.assertEquals(0, count);
      count++;
    }
  }

  static class C {
    static int count = 0;

    @Inject
    C() {
      Assert.assertEquals(0, count);
      count++;
    }
  }
}

class IncestuousInterfaceSingletons {
  interface AI {
  }

  interface BI {
  }

  interface CI {
  }

  static class A implements AI {
    static int count = 0;

    @Inject
    A(CI c, BI b) {
      Assert.assertEquals(0, count);
      count++;
    }
  }

  static class B implements BI {
    static int count = 0;

    protected B() {
    }

    @Inject
    B(CI c) {
      Assert.assertEquals(0, count);
      count++;
    }
  }

  static class BN extends B {
    static int count = 0;

    @Inject
    BN(CI c) {
      super();
      Assert.assertEquals(0, count);
      count++;
    }
  }

  static class C implements CI {
    static int count = 0;

    @Inject
    C() {
      Assert.assertEquals(0, count);
      count++;
    }
  }
}

class IsBrokenClassInjectable {
  @Inject
  public IsBrokenClassInjectable() {
    throw new UnsupportedOperationException();
  }
}

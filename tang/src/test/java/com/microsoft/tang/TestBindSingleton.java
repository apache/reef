package com.microsoft.tang;

import static org.junit.Assert.assertTrue;

import javax.inject.Inject;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationFile;

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
    b.bindSingletonImplementation(A.class, B.class);
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
    assertTrue("Singleton and not singleton should not be the same", a1 != b1);

    final Injector injector2 = Tang.Factory.getTang().newInjector(src);
    final A a3 = injector2.getInstance(A.class);
    assertTrue(
        "Two different injectors should return two different singletons",
        a3 != a1);

    final Injector injector3 = injector2.createChildInjector();
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
    cb.bindSingletonImplementation(LateBoundVolatile.A.class,
        LateBoundVolatile.B.class);
    final Injector i = tang.newInjector(cb.build());
    i.bindVolatileInstance(LateBoundVolatile.C.class, new LateBoundVolatile.C());
    i.getInstance(LateBoundVolatile.A.class);
  }
  
  @Test
  public void testMultipleInjectorInstaceWithSingleton() throws BindException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindSingleton(AA.class);
  
    final Injector i1 = Tang.Factory.getTang().newInjector(cb.build());
    final Injector i2 = Tang.Factory.getTang().newInjector(cb.build());

    assertTrue("Different injectors should return different singleton object instances", i1.getInstance(AA.class) != i2.getInstance(AA.class));
  }

  @Test
  public void testLateBoundVolatileInstanceWithSingletonY()
      throws BindException, InjectionException {
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindSingleton(LateBoundVolatile.C.class);
    Injector i = tang.newInjector(cb.build());
    i.bindVolatileInstance(LateBoundVolatile.C.class, new LateBoundVolatile.C());
    i.getInstance(LateBoundVolatile.C.class);
  }

  @Test
  public void testLateBoundVolatileInstanceWithSingletonZ()
      throws BindException, InjectionException {
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindSingletonImplementation(LateBoundVolatile.B.class,
        LateBoundVolatile.B.class);
    Injector i = tang.newInjector(cb.build());
    i.bindVolatileInstance(LateBoundVolatile.C.class, new LateBoundVolatile.C());
    i.getInstance(LateBoundVolatile.B.class);
  }

  @Test
  public void testInbredSingletons() throws BindException, InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindSingletonImplementation(InbredSingletons.A.class,
        InbredSingletons.A.class);
    b.bindSingletonImplementation(InbredSingletons.B.class,
        InbredSingletons.B.class);
    b.bindSingletonImplementation(InbredSingletons.C.class,
        InbredSingletons.C.class);
    Injector i = t.newInjector(b.build());
    i.getInstance(InbredSingletons.A.class);
  }

  @Test
  public void testIncestuousSingletons() throws BindException,
      InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindSingletonImplementation(IncestuousSingletons.A.class,
        IncestuousSingletons.A.class);
    b.bindSingletonImplementation(IncestuousSingletons.B.class,
        IncestuousSingletons.B.class);
    b.bindSingletonImplementation(IncestuousSingletons.C.class,
        IncestuousSingletons.C.class);
    Injector i = t.newInjector(b.build());
    i.getInstance(IncestuousSingletons.A.class);
  }

  @Test
  public void testIncestuousSingletons2() throws BindException,
      InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindSingletonImplementation(IncestuousSingletons.A.class,
        IncestuousSingletons.A.class);
    b.bindImplementation(IncestuousSingletons.B.class,
        IncestuousSingletons.BN.class);
    b.bindSingletonImplementation(IncestuousSingletons.C.class,
        IncestuousSingletons.C.class);
    Injector i = t.newInjector(b.build());
    i.getInstance(IncestuousSingletons.A.class);
  }

  @Test
  public void testIncestuousInterfaceSingletons() throws BindException,
      InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindSingletonImplementation(IncestuousInterfaceSingletons.AI.class,
        IncestuousInterfaceSingletons.A.class);
    b.bindSingletonImplementation(IncestuousInterfaceSingletons.BI.class,
        IncestuousInterfaceSingletons.BN.class);
    b.bindSingletonImplementation(IncestuousInterfaceSingletons.CI.class,
        IncestuousInterfaceSingletons.C.class);
    Injector i = t.newInjector(b.build());
    i.getInstance(IncestuousInterfaceSingletons.AI.class);
  }

  @Test
  public void testIncestuousInterfaceSingletons2() throws BindException,
      InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindSingletonImplementation(IncestuousInterfaceSingletons.AI.class,
        IncestuousInterfaceSingletons.A.class);
    b.bindImplementation(IncestuousInterfaceSingletons.BI.class,
        IncestuousInterfaceSingletons.B.class);
    // TODO: Should we require bind(A,B), then bind(B,B) if B has subclasses?
    b.bindImplementation(IncestuousInterfaceSingletons.B.class,
        IncestuousInterfaceSingletons.B.class);
    b.bindSingletonImplementation(IncestuousInterfaceSingletons.CI.class,
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
    b.bindSingletonImplementation(IsBrokenClassInjectable.class,
        IsBrokenClassInjectable.class);
    Assert.assertTrue(t.newInjector(b.build()).isInjectable(
        IsBrokenClassInjectable.class));
  }
  @Test(expected=InjectionException.class)
  public void testBrokenSingletonClassCantInject() throws BindException, InjectionException {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder b = t.newConfigurationBuilder();
    b.bindSingletonImplementation(IsBrokenClassInjectable.class,
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

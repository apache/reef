package com.microsoft.tang;

import javax.inject.Inject;

import org.junit.Before;
import org.junit.Test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;

public class TestTang {
  Tang tang;

  @Before
  public void setUp() throws Exception {
    MustBeSingleton.alreadyInstantiated = false;
    tang = Tang.Factory.getTang();
  }

  @Test
  public void testSingleton() throws BindException, InjectionException {
    ConfigurationBuilder t = tang.newConfigurationBuilder();
    t.bindSingleton(MustBeSingleton.class);
    t.register(TwoSingletons.class);
    tang.newInjector(t.build()).getInstance(TwoSingletons.class);
  }

  @Test(expected = InjectionException.class)
  public void testNotSingleton() throws NameResolutionException,
      ReflectiveOperationException, BindException, InjectionException {
    ConfigurationBuilder t = tang.newConfigurationBuilder();
    t.register(TwoSingletons.class);
    Injector injector = tang.newInjector(t.build());
    injector.getInstance(TwoSingletons.class);
  }

  @Test(expected = BindException.class)
  public void testRepeatedAmbiguousArgs() throws BindException {
    ConfigurationBuilder t = tang.newConfigurationBuilder();
    t.register(RepeatedAmbiguousArgs.class);
  }

  @Test
  public void testRepeatedOKArgs() throws BindException, InjectionException {
    ConfigurationBuilder t = tang.newConfigurationBuilder();
    t.bindNamedParameter(RepeatedNamedArgs.A.class, "1");
    t.bindNamedParameter(RepeatedNamedArgs.B.class, "2");
    Injector injector = tang.newInjector(t.build());
    injector.getInstance(RepeatedNamedArgs.class);
  }

  // NamedParameter A has no default_value, so this should throw.
  @Test(expected = InjectionException.class)
  public void testOneNamedFailArgs() throws BindException,
      InjectionException {
    ConfigurationBuilder t = tang.newConfigurationBuilder();
    t.register(OneNamedSingletonArgs.class);
    tang.newInjector(t.build())
        .getInstance(OneNamedSingletonArgs.class);
  }
  @Test(expected = InjectionException.class)
  public void testOneNamedOKArgs() throws BindException,
      InjectionException {
    ConfigurationBuilder t = tang.newConfigurationBuilder();
    t.register(OneNamedSingletonArgs.class);
    tang.newInjector(t.build())
        .getInstance(OneNamedSingletonArgs.class);
  }
  // NamedParameter A has no default_value, so this should throw
  @Test(expected = InjectionException.class)
  public void testOneNamedSingletonFailArgs() throws BindException,
      InjectionException {
    ConfigurationBuilder t = tang.newConfigurationBuilder();
    t.bindSingleton(MustBeSingleton.class);
    t.register(OneNamedSingletonArgs.class);
    tang.newInjector(t.build())
        .getInstance(OneNamedSingletonArgs.class);
  }
  // NamedParameter A get's bound to a volatile, so this should succeed.
  @Test
  public void testOneNamedSingletonOKArgs() throws BindException,
      InjectionException {
    ConfigurationBuilder t = tang.newConfigurationBuilder();
    t.register(OneNamedSingletonArgs.class);
    Injector i = tang.newInjector(t.build());
    i.bindVolatileParameter(OneNamedSingletonArgs.A.class, i.getInstance(MustBeSingleton.class));
    i.getInstance(OneNamedSingletonArgs.class);
  }
  @Test
  public void testRepeatedNamedOKArgs() throws BindException,
      InjectionException {
    ConfigurationBuilder t = tang.newConfigurationBuilder();
    t.bindSingleton(MustBeSingleton.class);
    t.register(RepeatedNamedSingletonArgs.class);
    Injector i = tang.newInjector(t.build());
    i.bindVolatileParameter(RepeatedNamedSingletonArgs.A.class, i.getInstance(MustBeSingleton.class));
    i.bindVolatileParameter(RepeatedNamedSingletonArgs.B.class, i.getInstance(MustBeSingleton.class));
    i.getInstance(RepeatedNamedSingletonArgs.class);
  }

  // Forgot to call bindSingleton
  @Test(expected = InjectionException.class)
  public void testRepeatedNamedFailArgs() throws BindException,
      InjectionException {
    ConfigurationBuilder t = tang.newConfigurationBuilder();
    t.register(RepeatedNamedSingletonArgs.class);
    Injector i = tang.newInjector(t.build());
    i.bindVolatileParameter(RepeatedNamedSingletonArgs.A.class, i.getInstance(MustBeSingleton.class));
    i.bindVolatileParameter(RepeatedNamedSingletonArgs.B.class, i.getInstance(MustBeSingleton.class));
    i.getInstance(RepeatedNamedSingletonArgs.class);
  }
  @Test
  public void testStraightforwardBuild() throws BindException, InjectionException {
    ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bind(Interf.class, Impl.class);
    tang.newInjector(cb.build()).getInstance(Interf.class);
  }
}

@NamedParameter(doc = "woo", short_name = "woo", default_value = "42")
class Param implements Name<Integer> {}
interface Interf { }
class Impl implements Interf {
  @Inject Impl(@Parameter(Param.class) int p) { } 
}

class MustBeSingleton {
  static boolean alreadyInstantiated;

  @Inject
  public MustBeSingleton() {
    if (alreadyInstantiated) {
      throw new IllegalStateException("Can't instantiate me twice!");
    }
    alreadyInstantiated = true;
  }
}

class SubSingleton {
  @Inject
  SubSingleton(MustBeSingleton a) {
  }
}

class TwoSingletons {
  @Inject
  TwoSingletons(SubSingleton a, MustBeSingleton b) {
  }
}

class RepeatedAmbiguousArgs {
  @Inject
  RepeatedAmbiguousArgs(int x, int y) {
  }
}

class RepeatedNamedArgs {
  @NamedParameter()
  class A implements Name<Integer> {
  }

  @NamedParameter()
  class B implements Name<Integer> {
  }

  @Inject
  RepeatedNamedArgs(@Parameter(A.class) int x, @Parameter(B.class) int y) {
  }
}

class RepeatedNamedSingletonArgs {
  @NamedParameter()
  class A implements Name<MustBeSingleton> {
  }

  @NamedParameter()
  class B implements Name<MustBeSingleton> {
  }

  @Inject
  RepeatedNamedSingletonArgs(@Parameter(A.class) MustBeSingleton a,
      @Parameter(B.class) MustBeSingleton b) {
  }
}
class OneNamedSingletonArgs {
  @NamedParameter()
  class A implements Name<MustBeSingleton> {
  }

  @NamedParameter()
  class B implements Name<MustBeSingleton> {
  }

  @Inject
  OneNamedSingletonArgs(@Parameter(A.class) MustBeSingleton a) {
  }
}

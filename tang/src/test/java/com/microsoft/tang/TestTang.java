package com.microsoft.tang;

import java.lang.reflect.InvocationTargetException;

import javax.inject.Inject;

import org.junit.Before;
import org.junit.Test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.ConfigurationBuilderImpl;
import com.microsoft.tang.implementation.InjectorImpl;

public class TestTang {
  @Before
  public void setUp() throws Exception {
    MustBeSingleton.alreadyInstantiated = false;
  }
  @Test
  public void testSingleton() throws NameResolutionException, ReflectiveOperationException {
    ConfigurationBuilderImpl t = new ConfigurationBuilderImpl();
    t.bindSingleton(MustBeSingleton.class);
    new InjectorImpl(t.build()).getInstance(TwoSingletons.class);
  }
  @Test(expected = InvocationTargetException.class)
  public void testNotSingleton() throws NameResolutionException, ReflectiveOperationException {
    ConfigurationBuilderImpl t = new ConfigurationBuilderImpl();
    InjectorImpl injector = new InjectorImpl(t.build());
    injector.getInstance(TwoSingletons.class);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testRepeatedAmbiguousArgs() {
    ConfigurationBuilderImpl t = new ConfigurationBuilderImpl();
    t.namespace.register(RepeatedAmbiguousArgs.class);
  }
  @Test
  public void testRepeatedOKArgs() throws NameResolutionException, ReflectiveOperationException {
    ConfigurationBuilderImpl t = new ConfigurationBuilderImpl();
    t.bindNamedParameter(RepeatedNamedArgs.A.class, "1");
    t.bindNamedParameter(RepeatedNamedArgs.B.class, "2");
    new InjectorImpl(t.build()).getInstance(RepeatedNamedArgs.class);
  }
/*  @Test
  public void testRepeatedNamedOKArgs() throws NameResolutionException, ReflectiveOperationException {
    ConfigurationBuilderImpl t = new ConfigurationBuilderImpl();
    t.bindSingleton(MustBeSingleton.class);
    t.getInstance(RepeatedNamedSingletonArgs.class);
  }
  @Test
  public void testRepeatedNamedFailArgs() throws NameResolutionException, ReflectiveOperationException {
    ConfigurationBuilderImpl t = new ConfigurationBuilderImpl();
    t.getInstance(RepeatedNamedSingletonArgs.class);
  }*/
}

class MustBeSingleton {
  static boolean alreadyInstantiated;
  @Inject
  public MustBeSingleton() {
    if(alreadyInstantiated) {
      throw new IllegalStateException("Can't instantiate me twice!");
    }
    alreadyInstantiated = true;
  }
}
class SubSingleton {
  @Inject SubSingleton(MustBeSingleton a) { }
}
class TwoSingletons {
  @Inject TwoSingletons(SubSingleton a, MustBeSingleton b) { }
}
class RepeatedAmbiguousArgs {
  @Inject RepeatedAmbiguousArgs(int x, int y) {}
}
class RepeatedNamedArgs {
  @NamedParameter()
  class A implements Name<Integer> {}
  @NamedParameter()
  class B implements Name<Integer> {}
  @Inject RepeatedNamedArgs(@Parameter(A.class) int x,
      @Parameter(B.class) int y) {}
}
class RepeatedNamedSingletonArgs {
  @NamedParameter()
  class A implements Name<MustBeSingleton> {}
  @NamedParameter()
  class B implements Name<MustBeSingleton> {}
  @Inject RepeatedNamedSingletonArgs(
      @Parameter(A.class) MustBeSingleton a,
      @Parameter(B.class) MustBeSingleton b) { }
}

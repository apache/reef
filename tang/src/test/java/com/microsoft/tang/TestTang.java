package com.microsoft.tang;

import javax.inject.Inject;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.formats.ConfigurationFile;
import com.microsoft.tang.util.ReflectionUtilities;

public class TestTang {
  Tang tang;

  @Before
  public void setUp() throws Exception {
    MustBeSingleton.alreadyInstantiated = false;
    tang = Tang.Factory.getTang();
  }

  @Test
  public void testSingleton() throws BindException, InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    t.bindSingleton(MustBeSingleton.class);
    t.register(TwoSingletons.class);
    tang.newInjector(t.build()).getInstance(TwoSingletons.class);
  }

  @Test(expected = InjectionException.class)
  public void testNotSingleton() throws NameResolutionException,
      ReflectiveOperationException, BindException, InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    t.register(TwoSingletons.class);
    Injector injector = tang.newInjector(t.build());
    injector.getInstance(TwoSingletons.class);
  }

  @Test(expected = BindException.class)
  public void testRepeatedAmbiguousArgs() throws BindException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    t.register(RepeatedAmbiguousArgs.class);
  }

  @Test
  public void testRepeatedOKArgs() throws BindException, InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    t.bindNamedParameter(RepeatedNamedArgs.A.class, "1");
    t.bindNamedParameter(RepeatedNamedArgs.B.class, "2");
    Injector injector = tang.newInjector(t.build());
    injector.getInstance(RepeatedNamedArgs.class);
  }

  // NamedParameter A has no default_value, so this should throw.
  @Test(expected = InjectionException.class)
  public void testOneNamedFailArgs() throws BindException, InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    t.register(OneNamedSingletonArgs.class);
    tang.newInjector(t.build()).getInstance(OneNamedSingletonArgs.class);
  }

  @Test(expected = InjectionException.class)
  public void testOneNamedOKArgs() throws BindException, InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    t.register(OneNamedSingletonArgs.class);
    tang.newInjector(t.build()).getInstance(OneNamedSingletonArgs.class);
  }

  // NamedParameter A has no default_value, so this should throw
  @Test(expected = InjectionException.class)
  public void testOneNamedSingletonFailArgs() throws BindException,
      InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    t.bindSingleton(MustBeSingleton.class);
    t.register(OneNamedSingletonArgs.class);
    tang.newInjector(t.build()).getInstance(OneNamedSingletonArgs.class);
  }

  // NamedParameter A get's bound to a volatile, so this should succeed.
  @Test
  public void testOneNamedSingletonOKArgs() throws BindException,
      InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    t.register(OneNamedSingletonArgs.class);
    final Injector i = tang.newInjector(t.build());
    i.bindVolatileParameter(OneNamedSingletonArgs.A.class,
        i.getInstance(MustBeSingleton.class));
    i.getInstance(OneNamedSingletonArgs.class);
  }

  @Test
  public void testRepeatedNamedOKArgs() throws BindException,
      InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    t.bindSingleton(MustBeSingleton.class);
    t.register(RepeatedNamedSingletonArgs.class);
    final Injector i = tang.newInjector(t.build());
    i.bindVolatileParameter(RepeatedNamedSingletonArgs.A.class,
        i.getInstance(MustBeSingleton.class));
    i.bindVolatileParameter(RepeatedNamedSingletonArgs.B.class,
        i.getInstance(MustBeSingleton.class));
    i.getInstance(RepeatedNamedSingletonArgs.class);
  }

  // Forgot to call bindSingleton
  @Test(expected = InjectionException.class)
  public void testRepeatedNamedFailArgs() throws BindException,
      InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    t.register(RepeatedNamedSingletonArgs.class);
    Injector i = tang.newInjector(t.build());
    i.bindVolatileParameter(RepeatedNamedSingletonArgs.A.class,
        i.getInstance(MustBeSingleton.class));
    i.bindVolatileParameter(RepeatedNamedSingletonArgs.B.class,
        i.getInstance(MustBeSingleton.class));
    i.getInstance(RepeatedNamedSingletonArgs.class);
  }

  @Test
  public void testStraightforwardBuild() throws BindException,
      InjectionException {
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bind(Interf.class, Impl.class);
    tang.newInjector(cb.build()).getInstance(Interf.class);
  }

  @Test(expected = BindException.class)
  public void testOneNamedStringArgCantRebind() throws BindException,
      InjectionException {
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.register(OneNamedStringArg.class);
    OneNamedStringArg a = tang.newInjector(cb.build()).getInstance(
        OneNamedStringArg.class);
    Assert.assertEquals("default", a.s);
    cb.bindNamedParameter(OneNamedStringArg.A.class, "not default");
    Injector i = tang.newInjector(cb.build());
    Assert
        .assertEquals("not default", i.getInstance(OneNamedStringArg.class).s);
    i.bindVolatileParameter(OneNamedStringArg.A.class, "volatile");
    Assert.assertEquals("volatile", i.getInstance(OneNamedStringArg.class).s);
  }

  @Test
  public void testOneNamedStringArgBind() throws BindException,
      InjectionException {
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.register(OneNamedStringArg.class);
    OneNamedStringArg a = tang.newInjector(cb.build()).getInstance(
        OneNamedStringArg.class);
    Assert.assertEquals("default", a.s);
    cb.bindNamedParameter(OneNamedStringArg.A.class, "not default");
    Injector i = tang.newInjector(cb.build());
    Assert
        .assertEquals("not default", i.getInstance(OneNamedStringArg.class).s);
  }

  @Test
  public void testOneNamedStringArgVolatile() throws BindException,
      InjectionException {
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.register(OneNamedStringArg.class);
    OneNamedStringArg a = tang.newInjector(cb.build()).getInstance(
        OneNamedStringArg.class);
    Assert.assertEquals("default", a.s);
    Injector i = tang.newInjector(cb.build());
    i.bindVolatileParameter(OneNamedStringArg.A.class, "volatile");
    Assert.assertEquals("volatile", i.getInstance(OneNamedStringArg.class).s);
  }

  @Test
  public void testTwoNamedStringArgsBind() throws BindException,
      InjectionException {
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.register(TwoNamedStringArgs.class);
    TwoNamedStringArgs a = tang.newInjector(cb.build()).getInstance(
        TwoNamedStringArgs.class);
    Assert.assertEquals("defaultA", a.a);
    Assert.assertEquals("defaultB", a.b);
    cb.bindNamedParameter(TwoNamedStringArgs.A.class, "not defaultA");
    cb.bindNamedParameter(TwoNamedStringArgs.B.class, "not defaultB");
    Injector i = tang.newInjector(cb.build());
    Assert.assertEquals("not defaultA",
        i.getInstance(TwoNamedStringArgs.class).a);
    Assert.assertEquals("not defaultB",
        i.getInstance(TwoNamedStringArgs.class).b);
  }

  @Test
  public void testTwoNamedStringArgsBindVolatile() throws BindException,
      InjectionException {
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.register(TwoNamedStringArgs.class);
    TwoNamedStringArgs a = tang.newInjector(cb.build()).getInstance(
        TwoNamedStringArgs.class);
    Assert.assertEquals("defaultA", a.a);
    Assert.assertEquals("defaultB", a.b);
    final Injector i = tang.newInjector(cb.build());
    i.bindVolatileParameter(TwoNamedStringArgs.A.class, "not defaultA");
    i.bindVolatileParameter(TwoNamedStringArgs.B.class, "not defaultB");
    Assert.assertEquals("not defaultA",
        i.getInstance(TwoNamedStringArgs.class).a);
    Assert.assertEquals("not defaultB",
        i.getInstance(TwoNamedStringArgs.class).b);

  }

  @Test(expected = BindException.class)
  public void testTwoNamedStringArgsReBindVolatileFail() throws BindException,
      InjectionException {
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.register(TwoNamedStringArgs.class);
    TwoNamedStringArgs a = tang.newInjector(cb.build()).getInstance(
        TwoNamedStringArgs.class);
    Assert.assertEquals("defaultA", a.a);
    Assert.assertEquals("defaultB", a.b);
    cb.bindNamedParameter(TwoNamedStringArgs.A.class, "not defaultA");
    cb.bindNamedParameter(TwoNamedStringArgs.B.class, "not defaultB");
    Injector i = tang.newInjector(cb.build());
    i.bindVolatileParameter(TwoNamedStringArgs.A.class, "not defaultA");
    i.bindVolatileParameter(TwoNamedStringArgs.B.class, "not defaultB");
  }

  @Test
  public void testBextendsAinjectA() throws BindException, InjectionException {
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bind(BextendsAinjectA.A.class, BextendsAinjectA.A.class);
    tang.newInjector(cb.build()).getInstance(BextendsAinjectA.A.class);
  }

  @Test
  public void testExternalConstructor() throws BindException,
      InjectionException {
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindConstructor(ExternalConstructorExample.Legacy.class,
        ExternalConstructorExample.LegacyWrapper.class);
    Injector i = tang.newInjector(cb.build());
    i.bindVolatileInstance(Integer.class, 42);
    i.bindVolatileInstance(String.class, "The meaning of life is ");
    ExternalConstructorExample.Legacy l = i
        .getInstance(ExternalConstructorExample.Legacy.class);
    Assert.assertEquals(new Integer(42), l.x);
    Assert.assertEquals("The meaning of life is ", l.y);

  }

  @Test
  public void testLegacyConstructor() throws BindException, InjectionException {
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.registerLegacyConstructor(
        ReflectionUtilities.getFullName(LegacyConstructor.class),
        ReflectionUtilities.getFullName(Integer.class),
        ReflectionUtilities.getFullName(String.class));
    cb.bind(LegacyConstructor.class, LegacyConstructor.class);
    String confString = ConfigurationFile.toConfigurationString(cb.build());
    JavaConfigurationBuilder cb2 = tang.newConfigurationBuilder();
    // System.err.println(confString);
    ConfigurationFile.addConfiguration(cb2, confString);
    Injector i = tang.newInjector(cb2.build());
    i.bindVolatileInstance(Integer.class, 42);
    i.bindVolatileInstance(String.class, "The meaning of life is ");
    LegacyConstructor l = i.getInstance(LegacyConstructor.class);
    Assert.assertEquals(new Integer(42), l.x);
    Assert.assertEquals("The meaning of life is ", l.y);

  }

  @Test
  public void testNamedImpl() throws BindException, InjectionException {
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(NamedImpl.AImplName.class, NamedImpl.Aimpl.class);
    cb.bindNamedParameter(NamedImpl.BImplName.class, NamedImpl.Bimpl.class);
    Injector i = tang.newInjector(cb.build());
    NamedImpl.Aimpl a1 = (NamedImpl.Aimpl) i
        .getNamedInstance(NamedImpl.AImplName.class);
    NamedImpl.Aimpl a2 = (NamedImpl.Aimpl) i
        .getNamedInstance(NamedImpl.AImplName.class);
    NamedImpl.Bimpl b1 = (NamedImpl.Bimpl) i
        .getNamedInstance(NamedImpl.BImplName.class);
    NamedImpl.Bimpl b2 = (NamedImpl.Bimpl) i
        .getNamedInstance(NamedImpl.BImplName.class);
    Assert.assertNotSame(a1, a2);
    Assert.assertNotSame(b1, b2);
  }
  @Test
  public void testUnit() throws BindException, InjectionException {
    Injector inj = tang.newInjector();
    OuterUnit.InA a = inj.getInstance(OuterUnit.InA.class);
    OuterUnit.InB b = inj.getInstance(OuterUnit.InB.class);
    Assert.assertEquals(a.slf, b.slf);
  }
}

@NamedParameter(doc = "woo", short_name = "woo", default_value = "42")
class Param implements Name<Integer> {
}

interface Interf {
}

class Impl implements Interf {
  @Inject
  Impl(@Parameter(Param.class) int p) {
  }
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

class OneNamedStringArg {
  @NamedParameter(default_value = "default")
  class A implements Name<String> {
  }

  public final String s;

  @Inject
  OneNamedStringArg(@Parameter(A.class) String s) {
    this.s = s;
  }
}

class TwoNamedStringArgs {
  @NamedParameter(default_value = "defaultA")
  class A implements Name<String> {
  }

  @NamedParameter(default_value = "defaultB")
  class B implements Name<String> {
  }

  public final String a;
  public final String b;

  @Inject
  TwoNamedStringArgs(@Parameter(A.class) String a, @Parameter(B.class) String b) {
    this.a = a;
    this.b = b;
  }
}

class BextendsAinjectA {
  static class A {
    @Inject
    A() {
    }
  }

  static class B extends A {
  }
}

class ExternalConstructorExample {
  class Legacy {
    final Integer x;
    final String y;

    public Legacy(Integer x, String y) {
      this.x = x;
      this.y = y;
    }
  }

  static class LegacyWrapper implements ExternalConstructor<Legacy> {
    final Integer x;
    final String y;

    @Inject
    LegacyWrapper(Integer x, String y) {
      this.x = x;
      this.y = y;
    }

    @Override
    public Legacy newInstance() {
      return new ExternalConstructorExample().new Legacy(x, y);
    }

  }
}

class LegacyConstructor {
  final Integer x;
  final String y;

  public LegacyConstructor(Integer x, String y) {
    this.x = x;
    this.y = y;
  }
}

class NamedImpl {
  @NamedParameter
  static class AImplName implements Name<A> {
  }

  @NamedParameter
  static class BImplName implements Name<A> {
  }

  static interface A {
  }

  static class Aimpl implements A {
    @Inject
    Aimpl() {
    }
  }

  static class Bimpl implements A {
    @Inject
    Bimpl() {
    }
  }

  static class ABtaker {
    @Inject
    ABtaker(@Parameter(AImplName.class) A a, @Parameter(BImplName.class) A b) {
      Assert.assertTrue("AImplName must be instance of Aimpl",
          a instanceof Aimpl);
      Assert.assertTrue("BImplName must be instance of Bimpl",
          b instanceof Bimpl);
    }
  }
}
@Unit
class OuterUnit {
  final OuterUnit self;
  @Inject
  OuterUnit() { self = this; }
  class InA { 
    OuterUnit slf = self;
  }
  class InB {
    OuterUnit slf = self;
  }
}

package com.microsoft.tang;

import javax.inject.Inject;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.microsoft.tang.ThreeConstructors.TCFloat;
import com.microsoft.tang.ThreeConstructors.TCInt;
import com.microsoft.tang.ThreeConstructors.TCString;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.formats.ConfigurationFile;
import com.microsoft.tang.util.ReflectionUtilities;

public class TestTang {
  Tang tang;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    MustBeSingleton.alreadyInstantiated = false;
    tang = Tang.Factory.getTang();
  }

  @Test
  public void testSingleton() throws BindException, InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    tang.newInjector(t.build()).getInstance(TwoSingletons.class);
  }

  @Test//(expected = InjectionException.class)
  public void testNotSingleton() throws NameResolutionException,
      ReflectiveOperationException, BindException, InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    Injector injector = tang.newInjector(t.build());
    injector.getInstance(TwoSingletons.class);
  }

  // TODO: Delete this?  (It is handled in TestClassHierarchy!)
  @Test(expected = ClassHierarchyException.class)
  public void testRepeatedAmbiguousArgs() throws BindException, NameResolutionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    t.getClassHierarchy().getNode(ReflectionUtilities.getFullName(RepeatedAmbiguousArgs.class));
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
  @Test
  public void testOneNamedFailArgs() throws BindException, InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("Cannot inject com.microsoft.tang.OneNamedSingletonArgs: com.microsoft.tang.OneNamedSingletonArgs missing argument com.microsoft.tang.OneNamedSingletonArgs$A");
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    tang.newInjector(t.build()).getInstance(OneNamedSingletonArgs.class);
  }

  @Test
  public void testOneNamedOKArgs() throws BindException, InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("Cannot inject com.microsoft.tang.OneNamedSingletonArgs: com.microsoft.tang.OneNamedSingletonArgs missing argument com.microsoft.tang.OneNamedSingletonArgs$A");
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    tang.newInjector(t.build()).getInstance(OneNamedSingletonArgs.class);
  }

  // NamedParameter A has no default_value
  @Test
  public void testOneNamedSingletonFailArgs() throws BindException,
      InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("Cannot inject com.microsoft.tang.OneNamedSingletonArgs: com.microsoft.tang.OneNamedSingletonArgs missing argument com.microsoft.tang.OneNamedSingletonArgs$A");
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    tang.newInjector(t.build()).getInstance(OneNamedSingletonArgs.class);
  }

  // NamedParameter A get's bound to a volatile, so this should succeed.
  @Test
  public void testOneNamedSingletonOKArgs() throws BindException,
      InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    final Injector i = tang.newInjector(t.build());
    i.bindVolatileParameter(OneNamedSingletonArgs.A.class,
        i.getInstance(MustBeSingleton.class));
    i.getInstance(OneNamedSingletonArgs.class);
  }

  @Test
  public void testRepeatedNamedOKArgs() throws BindException,
      InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
    final Injector i = tang.newInjector(t.build());
    i.bindVolatileParameter(RepeatedNamedSingletonArgs.A.class,
        i.getInstance(MustBeSingleton.class));
    i.bindVolatileParameter(RepeatedNamedSingletonArgs.B.class,
        i.getInstance(MustBeSingleton.class));
    i.getInstance(RepeatedNamedSingletonArgs.class);
  }
  
  @Test
  public void testRepeatedNamedArgs() throws BindException,
      InjectionException {
    JavaConfigurationBuilder t = tang.newConfigurationBuilder();
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

  @Test//(expected = BindException.class)
  public void testOneNamedStringArgCantRebind() throws BindException,
      InjectionException {
    thrown.expect(BindException.class);
    thrown.expectMessage("Attempt to re-bind named parameter com.microsoft.tang.OneNamedStringArg$A.  Old value was [not default] new value is [volatile]");
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
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

  @Test//(expected = BindException.class)
  public void testTwoNamedStringArgsReBindVolatileFail() throws BindException,
      InjectionException {
    thrown.expect(BindException.class);
    thrown.expectMessage("Attempt to re-bind named parameter com.microsoft.tang.TwoNamedStringArgs$A.  Old value was [not defaultA] new value is [not defaultA]");
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
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
    Assert.assertSame(a1, a2);
    Assert.assertSame(b1, b2);
  }
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testWrongNamedImpl() throws BindException {
    thrown.expect(BindException.class);
    thrown.expectMessage("Name<com.microsoft.tang.NamedImpl$A> com.microsoft.tang.NamedImpl$AImplName cannot take non-subclass com.microsoft.tang.NamedImpl$Cimpl");
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter((Class)NamedImpl.AImplName.class, (Class)NamedImpl.Cimpl.class);
  }
  @Test
  public void testUnit() throws BindException, InjectionException {
    Injector inj = tang.newInjector();
    OuterUnit.InA a = inj.getInstance(OuterUnit.InA.class);
    OuterUnit.InB b = inj.getInstance(OuterUnit.InB.class);
    Assert.assertEquals(a.slf, b.slf);
  }
  @Test
  public void testThreeConstructors() throws BindException, InjectionException {
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(TCInt.class, "1");
    cb.bindNamedParameter(TCString.class, "s");
    ThreeConstructors tc = tang.newInjector(cb.build()).getInstance(ThreeConstructors.class);
    Assert.assertEquals(1, tc.i);
    Assert.assertEquals("s", tc.s);
    
    cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(TCInt.class, "1");
    tc = tang.newInjector(cb.build()).getInstance(ThreeConstructors.class);
    Assert.assertEquals(1, tc.i);
    Assert.assertEquals("default", tc.s);

    cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(TCString.class, "s");
    tc = tang.newInjector(cb.build()).getInstance(ThreeConstructors.class);
    Assert.assertEquals(-1, tc.i);
    Assert.assertEquals("s", tc.s);

    cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(TCFloat.class, "2");
    tc = tang.newInjector(cb.build()).getInstance(ThreeConstructors.class);
    Assert.assertEquals(-1, tc.i);
    Assert.assertEquals("default", tc.s);
    Assert.assertEquals(2.0f, tc.f);

    
  }

  @Test
  public void testThreeConstructorsAmbiguous() throws BindException, InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("Cannot inject com.microsoft.tang.ThreeConstructors Multiple ways to inject com.microsoft.tang.ThreeConstructors");

    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(TCString.class, "s");
    cb.bindNamedParameter(TCFloat.class, "-2");

    // Ambiguous; there is a constructor that takes a string, and another that
    // takes a float, but none that takes both.
    tang.newInjector(cb.build()).getInstance(ThreeConstructors.class);
  }
  
  @Test
  public void testDefaultImplementation() throws BindException, ClassHierarchyException, InjectionException {
    ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    @SuppressWarnings("unused")
    IfaceWithDefault iwd = i.getNamedInstance(IfaceWithDefaultName.class);
  }

  @Test
  public void testCantGetInstanceOfNamedParameter() throws BindException, InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("getInstance() called on Name com.microsoft.tang.IfaceWithDefaultName Did you mean to call getNamedInstance() instead?");
    ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    @SuppressWarnings("unused")
    IfaceWithDefaultName iwd = i.getInstance(IfaceWithDefaultName.class);
  }
  @Test
  public void testCanGetDefaultedInterface() throws BindException, InjectionException {
    Assert.assertNotNull(Tang.Factory.getTang().newInjector().getInstance(HaveDefaultImpl.class));
  }
  @Test
  public void testCanOverrideDefaultedInterface() throws BindException, InjectionException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(HaveDefaultImpl.class, OverrideDefaultImpl.class);
    Assert.assertTrue(Tang.Factory.getTang().newInjector(cb.build())
        .getInstance(HaveDefaultImpl.class) instanceof OverrideDefaultImpl);
  }
  @Test
  public void testCanGetStringDefaultedInterface() throws BindException, InjectionException {
    Assert.assertNotNull(Tang.Factory.getTang().newInjector().getInstance(HaveDefaultStringImpl.class));
  }
  @Test
  public void testCanOverrideStringDefaultedInterface() throws BindException, InjectionException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(HaveDefaultStringImpl.class, OverrideDefaultStringImpl.class);
    Assert.assertTrue(Tang.Factory.getTang().newInjector(cb.build())
        .getInstance(HaveDefaultStringImpl.class) instanceof OverrideDefaultStringImpl);
  }
  @Test
  public void testSingletonWithMultipleConstructors() throws BindException, InjectionException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(SMC.class, SingletonMultiConst.class);
    cb.bindNamedParameter(SingletonMultiConst.A.class, "foo");
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    i.getInstance(SMC.class);
  }
}

interface SMC { }

class SingletonMultiConst implements SMC {
  @NamedParameter
  class A implements Name<String> { }
  @NamedParameter
  class B implements Name<String> { }
  @Inject
  public SingletonMultiConst(@Parameter(A.class) String a) { }
  @Inject
  public SingletonMultiConst(@Parameter(A.class) String a, @Parameter(B.class) String b) { }
}

@DefaultImplementation(HaveDefaultImplImpl.class)
interface HaveDefaultImpl {}
class HaveDefaultImplImpl implements HaveDefaultImpl {
  @Inject
  HaveDefaultImplImpl() {}
}
class OverrideDefaultImpl implements HaveDefaultImpl {
  @Inject
  public OverrideDefaultImpl() {}
}


@DefaultImplementation(name="com.microsoft.tang.HaveDefaultStringImplImpl")
interface HaveDefaultStringImpl {}
class HaveDefaultStringImplImpl implements HaveDefaultStringImpl {
  @Inject
  HaveDefaultStringImplImpl() {}
}
class OverrideDefaultStringImpl implements HaveDefaultStringImpl {
  @Inject
  public OverrideDefaultStringImpl() {}
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
  @NamedParameter
  static class CImplName implements Name<C> {
  }

  static interface A {
  }
  static interface C {
    
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
  static class Cimpl implements C {
    @Inject
    Cimpl() {
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

class ThreeConstructors {

  final int i;
  final String s;
  final Float f;

  @NamedParameter
  static class TCInt implements Name<Integer> {}

  @NamedParameter
  static class TCString implements Name<String> {}

  @NamedParameter
  static class TCFloat implements Name<Float> {}

  @Inject
  ThreeConstructors(@Parameter(TCInt.class) int i, @Parameter(TCString.class) String s) { 
    this.i = i;
    this.s = s;
    this.f = -1.0f;
  }

  @Inject
  ThreeConstructors(@Parameter(TCString.class) String s) {
    this(-1, s);
  }

  @Inject
  ThreeConstructors(@Parameter(TCInt.class) int i) {
    this(i, "default");
  }

  @Inject
  ThreeConstructors(@Parameter(TCFloat.class) float f) {
    this.i = -1;
    this.s = "default";
    this.f = f;
  }
}
interface IfaceWithDefault {
}

class IfaceWithDefaultDefaultImpl implements IfaceWithDefault {
  @Inject
  IfaceWithDefaultDefaultImpl() {}
}

@NamedParameter(default_class=IfaceWithDefaultDefaultImpl.class)
class IfaceWithDefaultName implements Name<IfaceWithDefault> {
}

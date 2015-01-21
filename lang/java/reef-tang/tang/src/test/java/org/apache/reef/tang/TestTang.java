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
package org.apache.reef.tang;

import org.apache.reef.tang.ThreeConstructors.TCFloat;
import org.apache.reef.tang.ThreeConstructors.TCInt;
import org.apache.reef.tang.ThreeConstructors.TCString;
import org.apache.reef.tang.annotations.*;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.formats.ConfigurationFile;
import org.apache.reef.tang.util.ReflectionUtilities;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.inject.Inject;

interface SMC {
}

@DefaultImplementation(HaveDefaultImplImpl.class)
interface HaveDefaultImpl {
}

@DefaultImplementation(name = "org.apache.reef.tang.HaveDefaultStringImplImpl")
interface HaveDefaultStringImpl {
}

interface Interf {
}

interface IfaceWithDefault {
}

interface X<T> {
}

interface Bottle<Y> {

}

interface EventHandler<T> {
}

@DefaultImplementation(MyEventHandler.class)
interface MyEventHandlerIface extends EventHandler<Foo> {
}

interface SomeIface {
}

@DefaultImplementation(AHandlerImpl.class)
interface AHandler extends EventHandler<AH> {
}

@DefaultImplementation(BHandlerImpl.class)
interface BHandler extends EventHandler<BH> {
}

interface CheckChildIface {
}

public class TestTang {
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  Tang tang;

  @Before
  public void setUp() throws Exception {
    MustBeSingleton.alreadyInstantiated = false;
    tang = Tang.Factory.getTang();
  }

  @Test
  public void testSingleton() throws InjectionException {
    Injector injector = tang.newInjector();
    Assert.assertNotNull(injector.getInstance(TwoSingletons.class));
    Assert.assertNotNull(injector.getInstance(TwoSingletons.class));
  }

  @Test
  public void testNotSingleton() throws InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("Could not invoke constructor");
    Assert.assertNotNull(tang.newInjector().getInstance(TwoSingletons.class));
    tang.newInjector().getInstance(TwoSingletons.class);
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
  public void testOneNamedFailArgs() throws InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("Cannot inject org.apache.reef.tang.OneNamedSingletonArgs: org.apache.reef.tang.OneNamedSingletonArgs missing argument org.apache.reef.tang.OneNamedSingletonArgs$A");
    tang.newInjector().getInstance(OneNamedSingletonArgs.class);
  }

  @Test
  public void testOneNamedOKArgs() throws InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("Cannot inject org.apache.reef.tang.OneNamedSingletonArgs: org.apache.reef.tang.OneNamedSingletonArgs missing argument org.apache.reef.tang.OneNamedSingletonArgs$A");
    tang.newInjector().getInstance(OneNamedSingletonArgs.class);
  }

  // NamedParameter A has no default_value
  @Test
  public void testOneNamedSingletonFailArgs() throws InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("Cannot inject org.apache.reef.tang.OneNamedSingletonArgs: org.apache.reef.tang.OneNamedSingletonArgs missing argument org.apache.reef.tang.OneNamedSingletonArgs$A");
    tang.newInjector().getInstance(OneNamedSingletonArgs.class);
  }

  // NamedParameter A get's bound to a volatile, so this should succeed.
  @Test
  public void testOneNamedSingletonOKArgs() throws BindException, InjectionException {
    final Injector i = tang.newInjector();
    i.bindVolatileParameter(OneNamedSingletonArgs.A.class,
        i.getInstance(MustBeSingleton.class));
    i.getInstance(OneNamedSingletonArgs.class);
  }

  @Test
  public void testRepeatedNamedOKArgs() throws BindException,
      InjectionException {
    final Injector i = tang.newInjector();
    i.bindVolatileParameter(RepeatedNamedSingletonArgs.A.class,
        i.getInstance(MustBeSingleton.class));
    i.bindVolatileParameter(RepeatedNamedSingletonArgs.B.class,
        i.getInstance(MustBeSingleton.class));
    i.getInstance(RepeatedNamedSingletonArgs.class);
  }

  @Test
  public void testRepeatedNamedArgs() throws BindException,
      InjectionException {
    Injector i = tang.newInjector();
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

  @Test
  public void testOneNamedStringArgCantRebind() throws BindException,
      InjectionException {
    thrown.expect(BindException.class);
    thrown.expectMessage("Attempt to re-bind named parameter org.apache.reef.tang.OneNamedStringArg$A.  Old value was [not default] new value is [volatile]");
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
    OneNamedStringArg a = tang.newInjector().getInstance(
        OneNamedStringArg.class);
    Assert.assertEquals("default", a.s);
    Injector i = tang.newInjector();
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
    thrown.expectMessage("Attempt to re-bind named parameter org.apache.reef.tang.TwoNamedStringArgs$A.  Old value was [not defaultA] new value is [not defaultA]");
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

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testWrongNamedImpl() throws BindException {
    thrown.expect(BindException.class);
    thrown.expectMessage("Name<org.apache.reef.tang.NamedImpl$A> org.apache.reef.tang.NamedImpl$AImplName cannot take non-subclass org.apache.reef.tang.NamedImpl$Cimpl");
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter((Class) NamedImpl.AImplName.class, (Class) NamedImpl.Cimpl.class);
  }

  @Test
  public void testUnit() throws BindException, InjectionException {
    Injector inj = tang.newInjector();
    OuterUnit.InA a = inj.getInstance(OuterUnit.InA.class);
    OuterUnit.InB b = inj.getInstance(OuterUnit.InB.class);
    Assert.assertEquals(a.slf, b.slf);
  }

  @Test
  public void testMissedUnit() throws BindException, InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("Cannot inject org.apache.reef.tang.MissOuterUnit$InA: No known implementations / injectable constructors for org.apache.reef.tang.MissOuterUnit$InA");
    Injector inj = tang.newInjector();
    MissOuterUnit.InA a = inj.getInstance(MissOuterUnit.InA.class);
  }

  @Test
  public void testMissedUnitButWithInjectInnerClass() throws BindException, InjectionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Cannot @Inject non-static member class unless the enclosing class an @Unit.  Nested class is:org.apache.reef.tang.MissOuterUnit$InB");
    Injector inj = tang.newInjector();
    MissOuterUnit.InB b = inj.getInstance(MissOuterUnit.InB.class);
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
    Assert.assertEquals(2.0f, tc.f, 1e-9);
  }

  @Test
  public void testThreeConstructorsAmbiguous() throws BindException, InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("Cannot inject org.apache.reef.tang.ThreeConstructors Ambigous subplan org.apache.reef.tang.ThreeConstructors");
//    thrown.expectMessage("Cannot inject org.apache.reef.tang.ThreeConstructors Multiple ways to inject org.apache.reef.tang.ThreeConstructors");

    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(TCString.class, "s");
    cb.bindNamedParameter(TCFloat.class, "-2");

    // Ambiguous; there is a constructor that takes a string, and another that
    // takes a float, but none that takes both.
    tang.newInjector(cb.build()).getInstance(ThreeConstructors.class);
  }

  @Test
  public void testTwoConstructorsAmbiguous() throws BindException, InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("Cannot inject org.apache.reef.tang.TwoConstructors: Multiple infeasible plans: org.apache.reef.tang.TwoConstructors:");
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindNamedParameter(TCString.class, "s");
    cb.bindNamedParameter(TCInt.class, "1");

    tang.newInjector(cb.build()).getInstance(TwoConstructors.class);
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
    thrown.expectMessage("getInstance() called on Name org.apache.reef.tang.IfaceWithDefaultName Did you mean to call getNamedInstance() instead?");
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

  @Test
  public void testInjectInjector() throws InjectionException, BindException {
    Injector i = Tang.Factory.getTang().newInjector();
    InjectInjector ii = i.getInstance(InjectInjector.class);
    Assert.assertSame(i, ii.i);
  }

  @Test
  public void testProactiveFutures() throws InjectionException, BindException {
    Injector i = Tang.Factory.getTang().newInjector();
    IsFuture.instantiated = false;
    i.getInstance(NeedsFuture.class);
    Assert.assertTrue(IsFuture.instantiated);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testGenericEventHandlers() throws BindException, InjectionException {
    JavaConfigurationBuilder cba = Tang.Factory.getTang().newConfigurationBuilder();
    cba.bindNamedParameter(XName.class, (Class) XAA.class);
    Tang.Factory.getTang().newInjector(cba.build()).getNamedInstance(XName.class);
    JavaConfigurationBuilder cbb = Tang.Factory.getTang().newConfigurationBuilder();
    cbb.bindNamedParameter(XName.class, XBB.class);
    Tang.Factory.getTang().newInjector(cbb.build()).getNamedInstance(XName.class);
    JavaConfigurationBuilder cbc = Tang.Factory.getTang().newConfigurationBuilder();
    cbc.bindNamedParameter(XName.class, (Class) XCC.class);
    Tang.Factory.getTang().newInjector(cbc.build()).getNamedInstance(XName.class);
  }

  @Test
  public void testGenericEventHandlerDefaults() throws BindException, InjectionException {
    JavaConfigurationBuilder cba = Tang.Factory.getTang().newConfigurationBuilder();
    Tang.Factory.getTang().newInjector(cba.build()).getNamedInstance(XNameDA.class);
    JavaConfigurationBuilder cbb = Tang.Factory.getTang().newConfigurationBuilder();
    Tang.Factory.getTang().newInjector(cbb.build()).getNamedInstance(XNameDB.class);
    JavaConfigurationBuilder cbc = Tang.Factory.getTang().newConfigurationBuilder();
    Tang.Factory.getTang().newInjector(cbc.build()).getNamedInstance(XNameDC.class);
  }

  @Test
  public void testGenericEventHandlerDefaultsBadTreeIndirection() throws BindException, InjectionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("class org.apache.reef.tang.XNameDAA defines a default class org.apache.reef.tang.XCC with a raw type that does not extend of its target's raw type class org.apache.reef.tang.XBB");

    JavaConfigurationBuilder cba = Tang.Factory.getTang().newConfigurationBuilder();
    Tang.Factory.getTang().newInjector(cba.build()).getNamedInstance(XNameDAA.class);
  }

  @Test
  public void testGenericEventHandlerDefaultsGoodTreeIndirection() throws BindException, InjectionException {
    JavaConfigurationBuilder cba = Tang.Factory.getTang().newConfigurationBuilder();
    Tang.Factory.getTang().newInjector(cba.build()).getNamedInstance(XNameDDAA.class);
  }

  @Test
  public void testGenericUnrelatedGenericTypeParameters() throws BindException, InjectionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("class org.apache.reef.tang.WaterBottleName defines a default class org.apache.reef.tang.GasCan with a type that does not extend its target's type org.apache.reef.tang.Bottle<org.apache.reef.tang.Water");

    JavaConfigurationBuilder cba = Tang.Factory.getTang().newConfigurationBuilder();
    Tang.Factory.getTang().newInjector(cba.build()).getNamedInstance(WaterBottleName.class);
  }

  @Test
  public void testGenericInterfaceUnboundTypeParametersName() throws BindException, InjectionException {
    JavaConfigurationBuilder cba = Tang.Factory.getTang().newConfigurationBuilder();
    Tang.Factory.getTang().newInjector(cba.build()).getNamedInstance(FooEventHandler.class);
  }

  @Test
  public void testGenericInterfaceUnboundTypeParametersNameIface() throws BindException, InjectionException {
    JavaConfigurationBuilder cba = Tang.Factory.getTang().newConfigurationBuilder();
    Tang.Factory.getTang().newInjector(cba.build()).getNamedInstance(IfaceEventHandler.class);
  }

  @Test
  public void testGenericInterfaceUnboundTypeParametersIface() throws BindException, InjectionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("interface org.apache.reef.tang.MyEventHandlerIface declares its default implementation to be non-subclass class org.apache.reef.tang.MyEventHandler");

    JavaConfigurationBuilder cba = Tang.Factory.getTang().newConfigurationBuilder();
    Tang.Factory.getTang().newInjector(cba.build()).isInjectable(MyEventHandlerIface.class);
  }

  @Test
  public void testWantSomeHandlers() throws BindException, InjectionException {
    Tang.Factory.getTang().newInjector().getInstance(WantSomeHandlers.class);
  }

  @Test
  public void testWantSomeHandlersBadOrder() throws BindException, InjectionException {
    Injector i = Tang.Factory.getTang().newInjector();
    i.getInstance(AHandler.class);
    i.getInstance(BHandler.class);
    i.getInstance(WantSomeFutureHandlers.class);
  }

  @Test
  public void testWantSomeFutureHandlersAlreadyBoundVolatile() throws BindException, InjectionException {
    Injector i = Tang.Factory.getTang().newInjector();
    i.bindVolatileInstance(AHandler.class, new AHandlerImpl());
    i.bindVolatileInstance(BHandler.class, new BHandlerImpl());
    i.getInstance(WantSomeFutureHandlers.class);
  }

  @Test
  public void testWantSomeFutureHandlers() throws BindException, InjectionException {
    Tang.Factory.getTang().newInjector().getInstance(WantSomeFutureHandlers.class);
  }

  @Test
  public void testWantSomeFutureHandlersUnit() throws BindException, InjectionException {
    Tang.Factory.getTang().newInjector().getInstance(WantSomeFutureHandlersUnit.class);
  }

  @Test
  public void testWantSomeFutureHandlersName() throws BindException, InjectionException {
    Tang.Factory.getTang().newInjector().getInstance(WantSomeFutureHandlersName.class);
  }

  @Test
  public void testUnitMixedCanInject() throws BindException, InjectionException {
    //testing that you should be able to have @Unit and also static inner classes not included
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    Injector i = Tang.Factory.getTang().newInjector(cb.build());

    i.getInstance(OuterUnitWithStatic.InnerStaticClass2.class);
  }

  @Test
  public void testUnitMixedCantInject() throws BindException, InjectionException {
    thrown.expect(InjectionException.class);
    thrown.expectMessage("Cannot inject org.apache.reef.tang.OuterUnitWithStatic$InnerStaticClass: No known implementations / injectable constructors for org.apache.reef.tang.OuterUnitWithStatic$InnerStaticClass");

    //testing that you should be able to have @Unit and also static inner classes not included
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    Injector i = Tang.Factory.getTang().newInjector(cb.build());

    i.getInstance(OuterUnitWithStatic.InnerStaticClass.class);
  }

  @Test
  public void testForkWorks() throws BindException, InjectionException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bind(CheckChildIface.class, CheckChildImpl.class);

    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    Injector i1 = i.forkInjector();
    CheckChildIface c1 = i1.getInstance(CheckChildIface.class);
    Injector i2 = i.forkInjector();
    CheckChildIface c2 = i2.getInstance(CheckChildIface.class);
    Assert.assertTrue(c1 != c2);
  }

  @Test
  public void testReuseFailedInjector() throws BindException, InjectionException {
    Injector i = Tang.Factory.getTang().newInjector();
    try {
      i.getInstance(Fail.class);
      Assert.fail("Injecting Fail should not have worked!");
    } catch (InjectionException e) {
      i.getInstance(Pass.class);
    }
  }

  @Test
  public void testForksInjectorInConstructor() throws BindException, InjectionException {
    Injector i = Tang.Factory.getTang().newInjector();
    i.getInstance(ForksInjectorInConstructor.class);
  }

  /**
   * This is to test multiple inheritance case.
   * When a subclass is bound to an interface, it's instance will be created in injection
   * When a subsubclass is bound to the interface, the subsubclass instance will be created in injection
   *
   * @throws BindException
   * @throws InjectionException
   */
  @Test
  public void testMultiInheritanceMiddleClassFirst() throws BindException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(CheckChildIface.class, CheckChildImpl.class);
    final Injector i = Tang.Factory.getTang().newInjector(cb.build());
    final CheckChildIface o1 = i.getInstance(CheckChildIface.class);
    Assert.assertTrue(o1 instanceof CheckChildImpl);

    final JavaConfigurationBuilder cb2 = Tang.Factory.getTang().newConfigurationBuilder();
    cb2.bindImplementation(CheckChildIface.class, CheckChildImplImpl.class);
    final Injector i2 = Tang.Factory.getTang().newInjector(cb2.build());
    final CheckChildIface o2 = i2.getInstance(CheckChildIface.class);
    Assert.assertTrue(o2 instanceof CheckChildImplImpl);
  }

  /**
   * This is to test multiple inheritance case.
   * When CheckChildImplImpl is bound to an interface, the CheckChildImplImpl instance will be created in injection
   * When CheckChildImpl is then bound to the same interface, even class hierarchy already knows it has an subclass CheckChildImplImpl,
   * Tang will only look at the constructors in CheckChildImpl
   *
   * @throws BindException
   * @throws InjectionException
   */
  @Test
  public void testMultiInheritanceSubclassFirst() throws BindException, InjectionException {
    final JavaConfigurationBuilder cb2 = Tang.Factory.getTang().newConfigurationBuilder();
    cb2.bindImplementation(CheckChildIface.class, CheckChildImplImpl.class);
    final Injector i2 = Tang.Factory.getTang().newInjector(cb2.build());
    final CheckChildIface o2 = i2.getInstance(CheckChildIface.class);
    Assert.assertTrue(o2 instanceof CheckChildImplImpl);

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(CheckChildIface.class, CheckChildImpl.class);
    final Injector i = Tang.Factory.getTang().newInjector(cb.build());
    final CheckChildIface o1 = i.getInstance(CheckChildIface.class);
    Assert.assertTrue(o1 instanceof CheckChildImpl);
  }
}

class Fail {
  @Inject
  public Fail() {
    throw new UnsupportedOperationException();
  }
}

class Pass {
  @Inject
  public Pass() {
  }
}

class IsFuture {
  static boolean instantiated;

  @Inject
  IsFuture(NeedsFuture nf) {
    instantiated = true;
  }
}

class NeedsFuture {
  @Inject
  NeedsFuture(InjectionFuture<IsFuture> isFut) {
  }
}

class InjectInjector {
  public final Injector i;

  @Inject
  InjectInjector(Injector i) {
    this.i = i;
  }
}

class SingletonMultiConst implements SMC {
  @Inject
  public SingletonMultiConst(@Parameter(A.class) String a) {
  }

  @Inject
  public SingletonMultiConst(@Parameter(A.class) String a, @Parameter(B.class) String b) {
  }

  @NamedParameter
  class A implements Name<String> {
  }

  @NamedParameter
  class B implements Name<String> {
  }
}

class HaveDefaultImplImpl implements HaveDefaultImpl {
  @Inject
  HaveDefaultImplImpl() {
  }
}

class OverrideDefaultImpl implements HaveDefaultImpl {
  @Inject
  public OverrideDefaultImpl() {
  }
}

class HaveDefaultStringImplImpl implements HaveDefaultStringImpl {
  @Inject
  HaveDefaultStringImplImpl() {
  }
}

class OverrideDefaultStringImpl implements HaveDefaultStringImpl {
  @Inject
  public OverrideDefaultStringImpl() {
  }
}

@NamedParameter(doc = "woo", short_name = "woo", default_value = "42")
class Param implements Name<Integer> {
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
    // Does not call super
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
  @Inject
  RepeatedNamedArgs(@Parameter(A.class) int x, @Parameter(B.class) int y) {
  }

  @NamedParameter()
  class A implements Name<Integer> {
  }

  @NamedParameter()
  class B implements Name<Integer> {
  }
}

class RepeatedNamedSingletonArgs {
  @Inject
  RepeatedNamedSingletonArgs(@Parameter(A.class) MustBeSingleton a,
                             @Parameter(B.class) MustBeSingleton b) {
  }

  @NamedParameter()
  class A implements Name<MustBeSingleton> {
  }

  @NamedParameter()
  class B implements Name<MustBeSingleton> {
  }
}

class OneNamedSingletonArgs {
  @Inject
  OneNamedSingletonArgs(@Parameter(A.class) MustBeSingleton a) {
  }

  @NamedParameter()
  class A implements Name<MustBeSingleton> {
  }

  @NamedParameter()
  class B implements Name<MustBeSingleton> {
  }
}

class OneNamedStringArg {
  public final String s;

  @Inject
  OneNamedStringArg(@Parameter(A.class) String s) {
    this.s = s;
  }

  @NamedParameter(default_value = "default")
  class A implements Name<String> {
  }
}

class TwoNamedStringArgs {
  public final String a;
  public final String b;

  @Inject
  TwoNamedStringArgs(@Parameter(A.class) String a, @Parameter(B.class) String b) {
    this.a = a;
    this.b = b;
  }

  @NamedParameter(default_value = "defaultA")
  class A implements Name<String> {
  }

  @NamedParameter(default_value = "defaultB")
  class B implements Name<String> {
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

  class Legacy {
    final Integer x;
    final String y;

    public Legacy(Integer x, String y) {
      this.x = x;
      this.y = y;
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
  static interface A {
  }

  static interface C {

  }

  @NamedParameter
  static class AImplName implements Name<A> {
  }

  @NamedParameter
  static class BImplName implements Name<A> {
  }

  @NamedParameter
  static class CImplName implements Name<C> {
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
  OuterUnit() {
    self = this;
  }

  class InA {
    OuterUnit slf = self;
  }

  class InB {
    OuterUnit slf = self;
  }
}

class MissOuterUnit {

  final MissOuterUnit self;

  @Inject
  MissOuterUnit() {
    self = this;
  }

  class InA {
    MissOuterUnit slf = self;
  }

  class InB {
    MissOuterUnit slf = self;

    @Inject
    InB() {
    }
  }
}

class ThreeConstructors {

  final int i;
  final String s;
  final Float f;

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

  @NamedParameter
  static class TCInt implements Name<Integer> {
  }

  @NamedParameter
  static class TCString implements Name<String> {
  }

  @NamedParameter
  static class TCFloat implements Name<Float> {
  }
}

class TwoConstructors {

  final int i;
  final String s;

  @Inject
  TwoConstructors(@Parameter(TCInt.class) int i, @Parameter(TCString.class) String s) {
    this.i = i;
    this.s = s;
  }

  @Inject
  TwoConstructors(@Parameter(TCString.class) String s, @Parameter(TCInt.class) int i) {
    this.i = i;
    this.s = s;
  }

  @NamedParameter
  static class TCInt implements Name<Integer> {
  }

  @NamedParameter
  static class TCString implements Name<String> {
  }
}

class IfaceWithDefaultDefaultImpl implements IfaceWithDefault {
  @Inject
  IfaceWithDefaultDefaultImpl() {
  }
}

@NamedParameter(default_class = IfaceWithDefaultDefaultImpl.class)
class IfaceWithDefaultName implements Name<IfaceWithDefault> {
}

@NamedParameter
class XName implements Name<X<BB>> {
}

@NamedParameter(default_class = XAA.class)
class XNameDA implements Name<X<BB>> {
}

@NamedParameter(default_class = XBB.class)
class XNameDB implements Name<X<BB>> {
}

@NamedParameter(default_class = XCC.class)
class XNameDC implements Name<X<BB>> {
}

@NamedParameter(default_class = XCC.class)
class XNameDAA implements Name<XBB> {
}

@NamedParameter(default_class = XXBB.class)
class XNameDDAA implements Name<XBB> {
}

@DefaultImplementation(AA.class)
class AA {
  @Inject
  AA() {
  }
}

@DefaultImplementation(BB.class)
class BB extends AA {
  @Inject
  BB() {
  }
}

@DefaultImplementation(CC.class)
class CC extends BB {
  @Inject
  CC() {
  }
}

class XAA implements X<AA> {
  @Inject
  XAA(AA aa) {
  }
}

@DefaultImplementation(XBB.class)
class XBB implements X<BB> {
  @Inject
  XBB(BB aa) {
  }
}

class XXBB extends XBB {
  @Inject
  XXBB(BB aa) {
    super(aa);
  }
}

class XCC implements X<CC> {
  @Inject
  XCC(CC aa) {
  }
}

class WaterBottle implements Bottle<Water> {

}

class GasCan implements Bottle<Gas> {

}

class Water {
}

class Gas {
}

@NamedParameter(default_class = GasCan.class)
class WaterBottleName implements Name<Bottle<Water>> {
}

class MyEventHandler<T> implements EventHandler<T> {
  @Inject
  MyEventHandler() {
  }
}

@NamedParameter(default_class = MyEventHandler.class)
class FooEventHandler implements Name<EventHandler<Foo>> {
}

@NamedParameter(default_class = MyEventHandler.class)
class IfaceEventHandler implements Name<EventHandler<SomeIface>> {
}

class AH {
  @Inject
  AH() {
  }
}

class BH {
  @Inject
  BH() {
  }
}

class AHandlerImpl implements AHandler {
  @Inject
  AHandlerImpl() {
  }
}

class BHandlerImpl implements BHandler {
  @Inject
  BHandlerImpl() {
  }
}

@Unit
class DefaultHandlerUnit {
  @Inject
  DefaultHandlerUnit() {
  }

  @DefaultImplementation(AHandlerImpl.class)
  interface AHandler extends EventHandler<AH> {
  }

  @DefaultImplementation(BHandlerImpl.class)
  interface BHandler extends EventHandler<BH> {
  }

  class AHandlerImpl implements AHandler {
    AHandlerImpl() {
    }
  }

  class BHandlerImpl implements BHandler {
    BHandlerImpl() {
    }
  }
}

class WantSomeHandlers {
  @Inject
  WantSomeHandlers(AHandler a, BHandler b) {
  }
}

class WantSomeFutureHandlers {
  @Inject
  WantSomeFutureHandlers(InjectionFuture<AHandler> a, InjectionFuture<BHandler> b) {
  }
}

class WantSomeFutureHandlersUnit {
  @Inject
  WantSomeFutureHandlersUnit(InjectionFuture<DefaultHandlerUnit.AHandler> a, InjectionFuture<DefaultHandlerUnit.BHandler> b) {
  }
}

@NamedParameter(default_class = AHandlerImpl.class)
class AHandlerName implements Name<EventHandler<AH>> {
}

@NamedParameter(default_class = BHandlerImpl.class)
class BHandlerName implements Name<EventHandler<BH>> {
}

class WantSomeFutureHandlersName {
  @Inject
  WantSomeFutureHandlersName(
      @Parameter(AHandlerName.class) InjectionFuture<EventHandler<AH>> a,
      @Parameter(BHandlerName.class) InjectionFuture<EventHandler<BH>> b) {
  }
}

@Unit
class OuterUnitWithStatic {

  @Inject
  public OuterUnitWithStatic() {
  }

  public void bar() {
    new InnerStaticClass().baz();
  }

  static class InnerStaticClass {
    public InnerStaticClass() {
    }

    public void baz() {
    }
  }

  static class InnerStaticClass2 {
    @Inject
    public InnerStaticClass2() {
    }

    public void baz() {
    }
  }

  public class InnerUnitClass {
    public void foo() {
    }
  }
}

class CheckChildImpl implements CheckChildIface {
  @Inject
  CheckChildImpl() {
  }
}

class CheckChildImplImpl extends CheckChildImpl {
  @Inject
  CheckChildImplImpl() {
  }
}

class ForksInjectorInConstructor {
  @Inject
  ForksInjectorInConstructor(Injector i) throws BindException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(Number.class, Integer.class);
    i.forkInjector(cb.build());
  }
}
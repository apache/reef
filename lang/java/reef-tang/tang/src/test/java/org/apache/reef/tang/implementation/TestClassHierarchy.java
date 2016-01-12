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
package org.apache.reef.tang.implementation;

import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.*;
import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.ClassHierarchySerializer;
import org.apache.reef.tang.types.ClassNode;
import org.apache.reef.tang.types.ConstructorDef;
import org.apache.reef.tang.types.Node;
import org.apache.reef.tang.util.ReflectionUtilities;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;

@DefaultImplementation(String.class)
interface BadIfaceDefault {
}

interface I1 {
}

public class TestClassHierarchy {
  protected ClassHierarchy ns;
  protected ClassHierarchySerializer serializer;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    TangImpl.reset();
    ns = Tang.Factory.getTang().getDefaultClassHierarchy();
    serializer = Tang.Factory.getTang().newInjector().getInstance(ClassHierarchySerializer.class);
  }

  public String s(final Class<?> c) {
    return ReflectionUtilities.getFullName(c);
  }

  @Test
  public void testJavaString() throws NameResolutionException {
    ns.getNode(ReflectionUtilities.getFullName(String.class));
    Node n = null;
    try {
      n = ns.getNode("java");
    } catch (final NameResolutionException expected) {
    }
    Assert.assertNull(n);
    try {
      n = ns.getNode("java.lang");
    } catch (final NameResolutionException expected) {
    }
    Assert.assertNull(n);
    Assert.assertNotNull(ns.getNode("java.lang.String"));
    try {
      ns.getNode("com.microsoft");
      Assert.fail("Didn't get expected exception");
    } catch (final NameResolutionException expected) {
    }
  }

  @Test
  public void testSimpleConstructors() throws NameResolutionException {
    final ClassNode<?> cls = (ClassNode<?>) ns.getNode(s(SimpleConstructors.class));
    Assert.assertTrue(cls.getChildren().size() == 0);
    final ConstructorDef<?>[] def = cls.getInjectableConstructors();
    Assert.assertEquals(3, def.length);
  }

  @Test
  public void testNamedParameterConstructors() throws NameResolutionException {
    ns.getNode(s(NamedParameterConstructors.class));
  }

  @Test
  public void testArray() throws NameResolutionException {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("No support for arrays, etc.  Name was: [Ljava.lang.String;");
    ns.getNode(s(new String[0].getClass()));
  }

  @Test
  public void testRepeatConstructorArg() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Repeated constructor parameter detected. " +
        " Cannot inject constructor org.apache.reef.tang.implementation.RepeatConstructorArg(int,int)");
    ns.getNode(s(RepeatConstructorArg.class));
  }

  @Test
  public void testRepeatConstructorArgClasses() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Repeated constructor parameter detected. " +
        " Cannot inject constructor org.apache.reef.tang.implementation.RepeatConstructorArgClasses" +
        "(org.apache.reef.tang.implementation.A,org.apache.reef.tang.implementation.A)");
    ns.getNode(s(RepeatConstructorArgClasses.class));
  }

  @Test
  public void testLeafRepeatedConstructorArgClasses() throws NameResolutionException {
    ns.getNode(s(LeafRepeatedConstructorArgClasses.class));
  }

  @Test
  public void testNamedRepeatConstructorArgClasses() throws NameResolutionException {
    ns.getNode(s(NamedRepeatConstructorArgClasses.class));
  }

  @Test
  public void testResolveDependencies() throws NameResolutionException {
    ns.getNode(s(SimpleConstructors.class));
    Assert.assertNotNull(ns.getNode(ReflectionUtilities
        .getFullName(String.class)));
  }

  @Test
  public void testDocumentedLocalNamedParameter() throws NameResolutionException {
    ns.getNode(s(DocumentedLocalNamedParameter.class));
  }

  @Test
  public void testNamedParameterTypeMismatch() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter type mismatch in " +
        "org.apache.reef.tang.implementation.NamedParameterTypeMismatch. " +
        " Constructor expects a java.lang.String but Foo is a java.lang.Integer");
    ns.getNode(s(NamedParameterTypeMismatch.class));
  }

  @Test
  public void testUnannotatedName() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter org.apache.reef.tang.implementation.UnannotatedName " +
        "is missing its @NamedParameter annotation.");
    ns.getNode(s(UnannotatedName.class));
  }

  // TODO: The next three error messages should be more specific about the underlying cause of the failure.
  @Test
  public void testAnnotatedNotName() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Found illegal @NamedParameter org.apache.reef.tang.implementation.AnnotatedNotName " +
        "does not implement Name<?>");
    ns.getNode(s(AnnotatedNotName.class));
  }

  @Test
  public void testAnnotatedNameWrongInterface() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Found illegal @NamedParameter " +
        "org.apache.reef.tang.implementation.AnnotatedNameWrongInterface does not implement Name<?>");
    ns.getNode(s(AnnotatedNameWrongInterface.class));
  }

  @Test
  public void testAnnotatedNameNotGenericInterface() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Found illegal @NamedParameter " +
        "org.apache.reef.tang.implementation.AnnotatedNameNotGenericInterface does not implement Name<?>");
    ns.getNode(s(AnnotatedNameNotGenericInterface.class));
  }

  @Test
  public void testAnnotatedNameMultipleInterfaces() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter org.apache.reef.tang.implementation.AnnotatedNameMultipleInterfaces " +
        "implements multiple interfaces.  It is only allowed to implement Name<T>");
    ns.getNode(s(AnnotatedNameMultipleInterfaces.class));
  }

  @Test
  public void testUnAnnotatedNameMultipleInterfaces() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter org.apache.reef.tang.implementation.UnAnnotatedNameMultipleInterfaces " +
        "is missing its @NamedParameter annotation.");
    ns.getNode(s(UnAnnotatedNameMultipleInterfaces.class));
  }

  @Test
  public void testNameWithConstructor() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter org.apache.reef.tang.implementation.NameWithConstructor has a constructor. " +
        " Named parameters must not declare any constructors.");
    ns.getNode(s(NameWithConstructor.class));
  }

  @Test
  public void testNameWithZeroArgInject() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter org.apache.reef.tang.implementation.NameWithZeroArgInject has " +
        "an injectable constructor.  Named parameters must not declare any constructors.");
    ns.getNode(s(NameWithZeroArgInject.class));
  }

  @Test
  public void testGenericTorture1() throws NameResolutionException {
    ns.getNode(s(GenericTorture1.class));
  }

  @Test
  public void testGenericTorture2() throws NameResolutionException {
    ns.getNode(s(GenericTorture2.class));
  }

  @Test
  public void testGenericTorture3() throws NameResolutionException {
    ns.getNode(s(GenericTorture3.class));
  }

  @Test
  public void testGenericTorture4() throws NameResolutionException {
    ns.getNode(s(GenericTorture4.class));
  }

  @Test
  public void testGenericTorture5() throws NameResolutionException {
    ns.getNode(s(GenericTorture5.class));
  }

  @Test
  public void testGenericTorture6() throws NameResolutionException {
    ns.getNode(s(GenericTorture6.class));
  }

  @Test
  public void testGenericTorture7() throws NameResolutionException {
    ns.getNode(s(GenericTorture7.class));
  }

  @Test
  public void testGenericTorture8() throws NameResolutionException {
    ns.getNode(s(GenericTorture8.class));
  }

  @Test
  public void testGenericTorture9() throws NameResolutionException {
    ns.getNode(s(GenericTorture9.class));
  }

  @Test
  public void testGenericTorture10() throws NameResolutionException {
    ns.getNode(s(GenericTorture10.class));
  }

  @Test
  public void testGenericTorture11() throws NameResolutionException {
    ns.getNode(s(GenericTorture11.class));
  }

  @Test
  public void testGenericTorture12() throws NameResolutionException {
    ns.getNode(s(GenericTorture12.class));
  }

  @Test
  public void testInjectNonStaticLocalArgClass() throws NameResolutionException {
    ns.getNode(s(InjectNonStaticLocalArgClass.class));
  }

  @Test(expected = ClassHierarchyException.class)
  public void testInjectNonStaticLocalType() throws NameResolutionException {
    ns.getNode(s(InjectNonStaticLocalType.class));
  }

  @Test(expected = ClassHierarchyException.class)
  public void testConflictingShortNames() throws NameResolutionException {
    ns.getNode(s(ShortNameFooA.class));
    ns.getNode(s(ShortNameFooB.class));
  }

  @Test
  public void testOKShortNames() throws NameResolutionException {
    ns.getNode(s(ShortNameFooA.class));
  }

  @Test
  public void testRoundTripInnerClassNames() throws ClassNotFoundException, NameResolutionException {
    final Node n = ns.getNode(s(Nested.Inner.class));
    Class.forName(n.getFullName());
  }

  @Test
  public void testRoundTripAnonInnerClassNames() throws ClassNotFoundException, NameResolutionException {
    final Node n = ns.getNode(s(AnonNested.x.getClass()));
    final Node m = ns.getNode(s(AnonNested.y.getClass()));
    Assert.assertNotSame(n.getFullName(), m.getFullName());
    final Class<?> c = Class.forName(n.getFullName());
    final Class<?> d = Class.forName(m.getFullName());
    Assert.assertNotSame(c, d);
  }

  @Test
  public void testUnitIsInjectable() throws InjectionException, NameResolutionException {
    final ClassNode<?> n = (ClassNode<?>) ns.getNode(s(OuterUnitTH.class));
    Assert.assertTrue(n.isUnit());
    Assert.assertTrue(n.isInjectionCandidate());
  }

  @Test
  public void testBadUnitDecl() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Detected explicit constructor in class enclosed in @Unit " +
        "org.apache.reef.tang.implementation.OuterUnitBad$InA  Such constructors are disallowed.");

    ns.getNode(s(OuterUnitBad.class));
  }

  @Test
  public void nameCantBindWrongSubclassAsDefault() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("class org.apache.reef.tang.implementation.BadName defines a default class " +
        "java.lang.Integer with a raw type that does not extend of its target's raw type class java.lang.String");

    ns.getNode(s(BadName.class));
  }

  @Test
  public void ifaceCantBindWrongImplAsDefault() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("interface org.apache.reef.tang.implementation.BadIfaceDefault declares " +
        "its default implementation to be non-subclass class java.lang.String");
    ns.getNode(s(BadIfaceDefault.class));
  }

  @Test
  public void testParseableDefaultClassNotOK() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter org.apache.reef.tang.implementation.BadParsableDefaultClass " +
        "defines default implementation for parsable type java.lang.String");
    ns.getNode(s(BadParsableDefaultClass.class));
  }

  @Test
  public void testDanglingUnit() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Class org.apache.reef.tang.implementation.DanglingUnit has an @Unit annotation, " +
        "but no non-static inner classes.  Such @Unit annotations would have no effect, and are therefore disallowed.");

    ns.getNode(s(DanglingUnit.class));

  }

  @Test
  public void testNonInjectableParam() throws NameResolutionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("org.apache.reef.tang.implementation.NonInjectableParam(int) is not injectable, " +
        "but it has an @Parameter annotation.");
    ns.getNode(s(NonInjectableParam.class));
  }

}

@NamedParameter(default_class = String.class)
class BadParsableDefaultClass implements Name<String> {
}

@NamedParameter(default_class = Integer.class)
class BadName implements Name<String> {
}

class SimpleConstructors {

  @Inject
  SimpleConstructors() {
  }

  @Inject
  SimpleConstructors(final int x) {
  }

  SimpleConstructors(final String x) {
  }

  @Inject
  SimpleConstructors(final int x, final String y) {
  }
}

class NamedParameterConstructors {

  @Inject
  NamedParameterConstructors(final String x, @Parameter(X.class) final String y) {
  }

  @NamedParameter()
  class X implements Name<String> {
  }
}

class RepeatConstructorArg {
  @Inject
  RepeatConstructorArg(final int x, final int y) {
  }
}

class A {
}

class RepeatConstructorArgClasses {
  @Inject
  RepeatConstructorArgClasses(final A x, final A y) {
  }
}

class LeafRepeatedConstructorArgClasses {

  static class A {
    class AA {
    }
  }

  static class B {
    class AA {
    }
  }

  static class C {
    @Inject
    C(final A.AA a, final B.AA b) {
    }
  }
}

@NamedParameter()
class AA implements Name<A> {
}

@NamedParameter()
class BB implements Name<A> {
}

class NamedRepeatConstructorArgClasses {
  @Inject
  NamedRepeatConstructorArgClasses(@Parameter(AA.class) final A x,
                                   @Parameter(BB.class) final A y) {
  }
}

class DocumentedLocalNamedParameter {

  @Inject
  DocumentedLocalNamedParameter(@Parameter(Foo.class) final String s) {
  }

  @NamedParameter(doc = "doc stuff", default_value = "some value")
  final class Foo implements Name<String> {
  }
}

class NamedParameterTypeMismatch {

  @Inject
  NamedParameterTypeMismatch(@Parameter(Foo.class) final String s) {
  }

  @NamedParameter(doc = "doc.stuff", default_value = "1")
  final class Foo implements Name<Integer> {
  }
}

class UnannotatedName implements Name<String> {
}

@NamedParameter(doc = "c")
class AnnotatedNotName {
}

@NamedParameter(doc = "c")
class AnnotatedNameWrongInterface implements I1 {
}

@SuppressWarnings("rawtypes")
@NamedParameter(doc = "c")
class AnnotatedNameNotGenericInterface implements Name {
}

class UnAnnotatedNameMultipleInterfaces implements Name<Object>, I1 {
}

@NamedParameter(doc = "c")
class AnnotatedNameMultipleInterfaces implements Name<Object>, I1 {
}

@NamedParameter()
final class NameWithConstructor implements Name<Object> {
  private NameWithConstructor(final int i) {
  }
}

@NamedParameter()
class NameWithZeroArgInject implements Name<Object> {
  @Inject
  NameWithZeroArgInject() {
  }
}

@NamedParameter()
class GenericTorture1 implements Name<Set<String>> {
}

@NamedParameter()
class GenericTorture2 implements Name<Set<?>> {
}

@NamedParameter()
class GenericTorture3 implements Name<Set<Set<String>>> {
}

@SuppressWarnings("rawtypes")
@NamedParameter()
class GenericTorture4 implements Name<Set<Set>> {
}

@NamedParameter()
class GenericTorture5 implements Name<Map<Set<String>, Set<String>>> {
}

@SuppressWarnings("rawtypes")
@NamedParameter()
class GenericTorture6 implements Name<Map<Set, Set<String>>> {
}

@SuppressWarnings("rawtypes")
@NamedParameter()
class GenericTorture7 implements Name<Map<Set<String>, Set>> {
}

@NamedParameter()
class GenericTorture8 implements Name<Map<String, String>> {
}

@SuppressWarnings("rawtypes")
@NamedParameter()
class GenericTorture9 implements Name<Map<Set, Set>> {
}

@NamedParameter()
class GenericTorture10 implements Name<List<String>> {
}

@NamedParameter()
class GenericTorture11 implements Name<List<?>> {
}

@NamedParameter()
class GenericTorture12 implements Name<List<List<String>>> {
}

class InjectNonStaticLocalArgClass {
  @Inject
  InjectNonStaticLocalArgClass(final NonStaticLocal x) {
  }

  class NonStaticLocal {
  }
}

class InjectNonStaticLocalType {
  class NonStaticLocal {
    @Inject
    NonStaticLocal(final NonStaticLocal x) {
    }
  }
}

@NamedParameter(short_name = "foo")
class ShortNameFooA implements Name<String> {
}

@NamedParameter(short_name = "foo")
class ShortNameFooB implements Name<Integer> {
}

class Nested {
  class Inner {
  }
}

@SuppressWarnings("checkstyle:hideutilityclassconstructor")
class AnonNested {
  protected static X x = new X() {
    @SuppressWarnings("unused")
    private int i;
  };
  protected static X y = new X() {
    @SuppressWarnings("unused")
    private int j;
  };

  interface X {
  }
}

@Unit
class OuterUnitBad {
  class InA {
    @Inject
    InA() {
    }
  }

  class InB {
  }
}

@Unit
class OuterUnitTH {
  class InA {
  }

  class InB {
  }
}

@Unit
class DanglingUnit {
  @Inject
  DanglingUnit() {
  }

  static class DoesntCount {
  }
}

class SomeName implements Name<Integer> {
}

class NonInjectableParam {
  NonInjectableParam(@Parameter(SomeName.class) final int i) {
  }
}

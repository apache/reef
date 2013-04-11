package com.microsoft.tang.implementation;

import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.microsoft.tang.JavaClassHierarchy;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.ReflectionUtilities;

public class TestTypeHierarchy {
  JavaClassHierarchy ns;

  @Rule public ExpectedException thrown = ExpectedException.none();
  
  @Before
  public void setUp() throws Exception {
    TangImpl.reset();
    ns = Tang.Factory.getTang().getDefaultClassHierarchy();
  }

  /**
   * TODO: How to handle getNode() that returns a package node? It breaks the
   * immutability of ClassHierarchy!
   */
  @Test
  public void testJavaString() throws NameResolutionException {
    ns.getNode(String.class);
    Node n = null;
    try {
      n = ns.getNode("java");
    } catch(NameResolutionException e) {
    }
    Assert.assertNull(n);
    try {
      n = ns.getNode("java.lang");
    } catch(NameResolutionException e) {
    }
    Assert.assertNull(n);
    Assert.assertNotNull(ns.getNode("java.lang.String"));
    try {
      ns.getNode("com.microsoft");
      Assert.fail("Didn't get expected exception");
    } catch (NameResolutionException e) {

    }
  }

  @Test
  public void testSimpleConstructors() {
    ClassNode<?> cls = (ClassNode<?>) ns.getNode(SimpleConstructors.class);
    Assert.assertTrue(cls.getChildren().size() == 0);
    ConstructorDef<?> def[] = cls.getInjectableConstructors();
    Assert.assertEquals(3, def.length);
  }

  @Test
  public void testNamedParameterConstructors() {
    ns.getNode(NamedParameterConstructors.class);
  }

  @Test
  public void testArray() {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("No support for arrays, etc.  Name was: [Ljava.lang.String;");
    ns.getNode(new String[0].getClass());
  }

  @Test
  public void testRepeatConstructorArg() {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Repeated constructor parameter detected.  Cannot inject constructor com.microsoft.tang.implementation.RepeatConstructorArg(int,int)");
    ns.getNode(RepeatConstructorArg.class);
  }

  @Test
  public void testRepeatConstructorArgClasses() {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Repeated constructor parameter detected.  Cannot inject constructor com.microsoft.tang.implementation.RepeatConstructorArgClasses(com.microsoft.tang.implementation.A,com.microsoft.tang.implementation.A)");
    ns.getNode(RepeatConstructorArgClasses.class);
  }

  @Test
  public void testLeafRepeatedConstructorArgClasses() {
    ns.getNode(LeafRepeatedConstructorArgClasses.class);
  }

  @Test
  public void testNamedRepeatConstructorArgClasses() {
    ns.getNode(NamedRepeatConstructorArgClasses.class);
  }

  @Test
  public void testResolveDependencies() throws NameResolutionException {
    ns.getNode(SimpleConstructors.class);
    Assert.assertNotNull(ns.getNode(ReflectionUtilities
        .getFullName(String.class)));
  }

  @Test
  public void testDocumentedLocalNamedParameter() {
    ns.getNode(DocumentedLocalNamedParameter.class);
  }

  @Test
  public void testNamedParameterTypeMismatch() {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter type mismatch.  Constructor expects a java.lang.String but Foo is a java.lang.Integer");
    ns.getNode(NamedParameterTypeMismatch.class);
  }

  @Test
  public void testUnannotatedName() {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter com.microsoft.tang.implementation.UnannotatedName is missing its @NamedParameter annotation.");
    ns.getNode(UnannotatedName.class);
  }

  // TODO: The next three error messages should be more specific about the underlying cause of the failure.
  @Test
  public void testAnnotatedNotName() {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Found illegal @NamedParameter com.microsoft.tang.implementation.AnnotatedNotName does not implement Name<?>");
    ns.getNode(AnnotatedNotName.class);
  }

  @Test
  public void testAnnotatedNameWrongInterface() {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Found illegal @NamedParameter com.microsoft.tang.implementation.AnnotatedNameWrongInterface does not implement Name<?>");
    ns.getNode(AnnotatedNameWrongInterface.class);
  }

  @Test
  public void testAnnotatedNameNotGenericInterface() {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Found illegal @NamedParameter com.microsoft.tang.implementation.AnnotatedNameNotGenericInterface does not implement Name<?>");
    ns.getNode(AnnotatedNameNotGenericInterface.class);
  }

  @Test
  public void testAnnotatedNameMultipleInterfaces() {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter com.microsoft.tang.implementation.AnnotatedNameMultipleInterfaces implements multiple interfaces.  It is only allowed to implement Name<T>");
    ns.getNode(AnnotatedNameMultipleInterfaces.class);
  }

  @Test
  public void testUnAnnotatedNameMultipleInterfaces() {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter com.microsoft.tang.implementation.UnAnnotatedNameMultipleInterfaces is missing its @NamedParameter annotation.");
    ns.getNode(UnAnnotatedNameMultipleInterfaces.class);
  }

  @Test
  public void testNameWithConstructor() {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter com.microsoft.tang.implementation.NameWithConstructor has a constructor.  Named parameters must not declare any constructors.");
    ns.getNode(NameWithConstructor.class);
  }

  @Test
  public void testNameWithZeroArgInject() {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Named parameter com.microsoft.tang.implementation.NameWithZeroArgInject has an injectable constructor.  Named parameters must not declare any constructors.");
    ns.getNode(NameWithZeroArgInject.class);
  }

  @Test
  public void testGenericTorture1() {
    ns.getNode(GenericTorture1.class);
  }

  @Test
  public void testGenericTorture2() {
    ns.getNode(GenericTorture2.class);
  }

  @Test
  public void testGenericTorture3() {
    ns.getNode(GenericTorture3.class);
  }

  @Test
  public void testGenericTorture4() {
    ns.getNode(GenericTorture4.class);
  }

  @Test
  public void testGenericTorture5() {
    ns.getNode(GenericTorture5.class);
  }

  @Test
  public void testGenericTorture6() {
    ns.getNode(GenericTorture6.class);
  }

  @Test
  public void testGenericTorture7() {
    ns.getNode(GenericTorture7.class);
  }

  @Test
  public void testGenericTorture8() {
    ns.getNode(GenericTorture8.class);
  }

  @Test
  public void testGenericTorture9() {
    ns.getNode(GenericTorture9.class);
  }

  @Test
  public void testInjectNonStaticLocalArgClass() {
    ns.getNode(InjectNonStaticLocalArgClass.class);
  }

  @Test(expected = ClassHierarchyException.class)
  public void testInjectNonStaticLocalType() {
    ns.getNode(InjectNonStaticLocalType.class);
  }

  @Test(expected = ClassHierarchyException.class)
  public void testConflictingShortNames() {
    ns.getNode(ShortNameFooA.class);
    ns.getNode(ShortNameFooB.class);
  }

  @Test
  public void testOKShortNames() {
    ns.getNode(ShortNameFooA.class);
  }

  @Test
  public void testRoundTripInnerClassNames() throws ClassNotFoundException {
    Node n = ns.getNode(Nested.Inner.class);
    Class.forName(n.getFullName());
  }

  @Test
  public void testRoundTripAnonInnerClassNames() throws ClassNotFoundException {
    Node n = ns.getNode(AnonNested.x.getClass());
    Node m = ns.getNode(AnonNested.y.getClass());
    Assert.assertNotSame(n.getFullName(), m.getFullName());
    Class<?> c = Class.forName(n.getFullName());
    Class<?> d = Class.forName(m.getFullName());
    Assert.assertNotSame(c, d);
  }

  @Test
  public void testUnitIsInjectable() throws InjectionException {
    ClassNode<?> n = (ClassNode<?>) ns.getNode(OuterUnitTH.class);
    Assert.assertTrue(n.isUnit());
    Assert.assertTrue(n.isInjectionCandidate());
  }

  @Test
  public void testBadUnitDecl() {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Detected explicit constructor in class enclosed in @Unit com.microsoft.tang.implementation.OuterUnitBad$InA  Such constructors are disallowed.");

    ns.getNode(OuterUnitBad.class);
  }

}

class SimpleConstructors {
  @Inject
  public SimpleConstructors() {
  }

  @Inject
  public SimpleConstructors(int x) {
  }

  public SimpleConstructors(String x) {
  }

  @Inject
  public SimpleConstructors(int x, String y) {
  }
}

class NamedParameterConstructors {
  @NamedParameter()
  class X implements Name<String> {
  };

  @Inject
  public NamedParameterConstructors(String x, @Parameter(X.class) String y) {
  }
}

class RepeatConstructorArg {
  public @Inject
  RepeatConstructorArg(int x, int y) {
  }
}

class A {
}

class RepeatConstructorArgClasses {
  public @Inject
  RepeatConstructorArgClasses(A x, A y) {
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
    C(A.AA a, B.AA b) {
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
  public @Inject
  NamedRepeatConstructorArgClasses(@Parameter(AA.class) A x,
      @Parameter(BB.class) A y) {
  }
}

class DocumentedLocalNamedParameter {
  @NamedParameter(doc = "doc stuff", default_value = "some value")
  final class Foo implements Name<String> {
  }

  @Inject
  public DocumentedLocalNamedParameter(@Parameter(Foo.class) String s) {
  }
}

class NamedParameterTypeMismatch {
  @NamedParameter(doc = "doc.stuff", default_value = "1")
  final class Foo implements Name<Integer> {
  }

  @Inject
  public NamedParameterTypeMismatch(@Parameter(Foo.class) String s) {
  }
}

class UnannotatedName implements Name<String> {
}

interface I1 {
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
class NameWithConstructor implements Name<Object> {
  private NameWithConstructor(int i) {
  }
}

@NamedParameter()
class NameWithZeroArgInject implements Name<Object> {
  @Inject
  public NameWithZeroArgInject() {
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

class InjectNonStaticLocalArgClass {
  class NonStaticLocal {
  }

  @Inject
  InjectNonStaticLocalArgClass(NonStaticLocal x) {
  }
}

class InjectNonStaticLocalType {
  class NonStaticLocal {
    @Inject
    NonStaticLocal(NonStaticLocal x) {
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

class AnonNested {
  static interface X {
  }

  static X x = new X() {
    @SuppressWarnings("unused")
    int i;
  };
  static X y = new X() {
    @SuppressWarnings("unused")
    int j;
  };
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

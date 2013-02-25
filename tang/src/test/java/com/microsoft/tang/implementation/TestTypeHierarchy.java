package com.microsoft.tang.implementation;

import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Namespace;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.java.ClassHierarchyImpl;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.NamespaceNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.ReflectionUtilities;

public class TestTypeHierarchy {
  ClassHierarchyImpl ns;

  @Before
  public void setUp() throws Exception {
    ns = new ClassHierarchyImpl();
  }

  @After
  public void tearDown() throws Exception {
    ns = null;
  }

  private Node register(Class<?> c) throws BindException {
    return ns.register(ReflectionUtilities.getFullName(c));
  }
  
  @Test
  public void testJavaString() throws NameResolutionException, BindException {
    register(String.class);
    Assert.assertNotNull(ns.getNode("java"));
    Assert.assertNotNull(ns.getNode("java.lang"));
    Assert.assertNotNull(ns.getNode("java.lang.String"));
//    Assert.assertNotNull(ns.getNode(String.class));
    try {
      ns.getNode("com.microsoft");
      Assert.fail("Didn't get expected exception");
    } catch (NameResolutionException e) {

    }
    try {
      ns.getNode(ReflectionUtilities.getFullName(this.getClass()));
      Assert.fail("Didn't get expected exception");
    } catch (NameResolutionException e) {

    }
  }

  @Test
  public void testSimpleConstructors() throws NameResolutionException,
      BindException {
    register(SimpleConstructors.class);
    Assert.assertNotNull(ns.getNode(SimpleConstructors.class.getName()));
    ClassNode<?> cls = (ClassNode<?>) ns.getNode(ReflectionUtilities.getFullName(SimpleConstructors.class));
    Assert.assertTrue(cls.getChildren().size() == 0);
    ConstructorDef<?> def[] = cls.getInjectableConstructors();
    Assert.assertEquals(3, def.length);

  }

  @Test
  public void testNamedParameterConstructors() throws BindException {
    register(NamedParameterConstructors.class);
  }

  @Test
  public void testArray() throws BindException {
    register(new String[0].getClass());
  }

  @Test
  public void testMetadata() throws NameResolutionException, BindException {
    register(Metadata.class);
    Assert.assertNotNull(ns.getNode(ReflectionUtilities.getFullName(Metadata.class)));
    Assert.assertFalse(ns.getNode("foo.bar") instanceof NamedParameterNode);
    Assert.assertTrue(ns.getNode("foo.bar.Quuz") instanceof NamedParameterNode);
    Assert.assertTrue(((ClassNode<?>) ns.getNode(ReflectionUtilities.getFullName(Metadata.class)))
        .getIsPrefixTarget());
  }

  @Test(expected = BindException.class)
  public void testRepeatConstructorArg() throws BindException {
    register(RepeatConstructorArg.class);
  }

  @Test(expected = BindException.class)
  public void testRepeatConstructorArgClasses() throws BindException {
    register(RepeatConstructorArgClasses.class);
  }
  @Test
  public void testLeafRepeatedConstructorArgClasses() throws BindException {
    register(LeafRepeatedConstructorArgClasses.class);
  }
  @Test
  public void testNamedRepeatConstructorArgClasses() throws BindException {
    register(NamedRepeatConstructorArgClasses.class);
  }

  @Test
  public void testResolveDependencies() throws NameResolutionException,
      BindException {
    register(SimpleConstructors.class);
    Assert.assertNotNull(ns.getNode(ReflectionUtilities.getFullName(String.class)));
  }

  @Test
  public void testDocumentedLocalNamedParameter() throws BindException {
    register(DocumentedLocalNamedParameter.class);
  }

  @Test(expected = BindException.class)
  public void testNamedParameterTypeMismatch() throws BindException {
    register(NamedParameterTypeMismatch.class);
  }

  @Test(expected = BindException.class)
  // Note: should pass when nested namespaces are working!
  public void testInconvenientNamespaceRegistrationOrder()
      throws NameResolutionException, BindException {
    register(InconvenientNamespaceRegistrationOrder1.class);
    Assert.assertTrue(ns.getNode("a.b") instanceof NamespaceNode);
    register(InconvenientNamespaceRegistrationOrder2.class);
    Assert.assertTrue(ns.getNode("a.b") instanceof NamespaceNode);
    Assert.assertTrue(ns.getNode("a") instanceof NamespaceNode);
    Assert.assertTrue(ns.getNode("a.B") instanceof NamedParameterNode);
    Assert.assertTrue(ns.getNode("a.b.C") instanceof NamedParameterNode);
  }

  @Test(expected = BindException.class)
  public void testNamespaceNamedParameterAnnotation() throws BindException {
    register(NamespaceNamedParameterAnnotation.class);
  }

  @Test(expected = BindException.class)
  public void testNamespaceNamedParameterNoAnnotation() throws BindException {
    register(NamespaceNamedParameterNoAnnotation.class);
  }

  @Test(expected = BindException.class)
  public void testUnannotatedName() throws BindException {
    register(UnannotatedName.class);
  }

  @Test(expected = BindException.class)
  public void testAnnotatedNotName() throws BindException {
    register(AnnotatedNotName.class);
  }

  @Test(expected = BindException.class)
  public void testAnnotatedNameWrongInterface() throws BindException {
    register(AnnotatedNameWrongInterface.class);
  }

  @Test(expected = BindException.class)
  public void testAnnotatedNameNotGenericInterface() throws BindException {
    register(AnnotatedNameNotGenericInterface.class);
  }

  @Test(expected = BindException.class)
  public void testAnnotatedNameMultipleInterfaces() throws BindException {
    register(AnnotatedNameMultipleInterfaces.class);
  }

  @Test(expected = BindException.class)
  public void testUnAnnotatedNameMultipleInterfaces() throws BindException {
    register(UnAnnotatedNameMultipleInterfaces.class);
  }

  @Test(expected = BindException.class)
  public void testNameWithConstructor() throws BindException {
    register(NameWithConstructor.class);
  }

  @Test(expected = BindException.class)
  public void testNameWithZeroArgInject() throws BindException {
    register(NameWithZeroArgInject.class);
  }

  @Test
  public void testGenericTorture1() throws BindException {
    register(GenericTorture1.class);
  }

  @Test
  public void testGenericTorture2() throws BindException {
    register(GenericTorture2.class);
  }

  @Test
  public void testGenericTorture3() throws BindException {
    register(GenericTorture3.class);
  }

  @Test
  public void testGenericTorture4() throws BindException {
    register(GenericTorture4.class);
  }

  @Test
  public void testGenericTorture5() throws BindException {
    register(GenericTorture5.class);
  }

  @Test
  public void testGenericTorture6() throws BindException {
    register(GenericTorture6.class);
  }

  @Test
  public void testGenericTorture7() throws BindException {
    register(GenericTorture7.class);
  }

  @Test
  public void testGenericTorture8() throws BindException {
    register(GenericTorture8.class);
  }

  @Test
  public void testGenericTorture9() throws BindException {
    register(GenericTorture9.class);
  }

  @Test
  public void testInjectNonStaticLocalArgClass() throws BindException {
    register(InjectNonStaticLocalArgClass.class);
  }

  @Test(expected = BindException.class)
  public void testInjectNonStaticLocalType() throws BindException {
    register(InjectNonStaticLocalType.class);
  }

  @Test(expected = BindException.class)
  public void testNamespacePointsToPackage() throws BindException {
    register(NamespacePointsToPackage.class);
  }

  @Test(expected = BindException.class)
  // Note: This should throw a BindException when nested namespaces are
  // implemented.
  public void testOverlappingNamespaces2() throws BindException {
    register(OverlappingNamespaces.class);
    register(OverlappingNamespaces2.class);
  }

  @Test(expected = BindException.class)
  // Note: This should throw a BindException when nested namespaces are
  // implemented.
  public void testOverlappingNamespaces2a() throws BindException {
    register(OverlappingNamespaces2.class);
    register(OverlappingNamespaces.class);
  }

  @Test(expected = BindException.class)
  // Note: This should *not* throw a BindException when nested namespaces are
  // implemented.
  public void testOverlappingNamespaces3() throws BindException {
    register(OverlappingNamespaces.class);
    register(OverlappingNamespaces3.class);
  }

  @Test(expected = BindException.class)
  public void testConflictingShortNames() throws BindException {
    register(ShortNameFooA.class);
    register(ShortNameFooB.class);
  }

  @Test
  public void testOKShortNames() throws BindException {
    register(ShortNameFooA.class);
  }

  @Test
  public void testRoundTripInnerClassNames() throws BindException, ClassNotFoundException {
    Node n = register(Nested.Inner.class);
    Class.forName(n.getFullName());
  }
  @Test
  public void testRoundTripAnonInnerClassNames() throws BindException, ClassNotFoundException {
    Node n = register(AnonNested.x.getClass());
    Node m = register(AnonNested.y.getClass());
    Assert.assertNotSame(n.getFullName(), m.getFullName());
    Class<?> c = Class.forName(n.getFullName());
    Class<?> d = Class.forName(m.getFullName());
    Assert.assertNotSame(c, d);
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

@Namespace("foo.bar")
class Metadata {
  @NamedParameter(doc = "a baz", default_value = "woo")
  final class Baz implements Name<String> {
  };

  @NamedParameter(doc = "a bar", default_value = "i-beam")
  final class Bar implements Name<String> {
  };

  @NamedParameter(doc = "???")
  final class Quuz implements Name<String> {
  };
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
  static class A { class AA { }}
  static class B { class AA { }}
  static class C { @Inject C(A.AA a, B.AA b) { }}
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

@Namespace("bar")
class DocumentedLocalNamedParameter {
  @NamedParameter(doc = "doc stuff", default_value = "some value")
  final class Foo implements Name<String> {
  }

  @Inject
  public DocumentedLocalNamedParameter(@Parameter(Foo.class) String s) {
  }
}

@Namespace("baz")
class NamedParameterTypeMismatch {
  @NamedParameter(doc = "doc.stuff", default_value = "1")
  final class Foo implements Name<Integer> {
  }

  @Inject
  public NamedParameterTypeMismatch(@Parameter(Foo.class) String s) {
  }
}

@Namespace("a.b")
class InconvenientNamespaceRegistrationOrder1 {
  @NamedParameter()
  final class C implements Name<String> {
  }
}

@Namespace("a")
class InconvenientNamespaceRegistrationOrder2 {
  @NamedParameter()
  final class B implements Name<String> {
  }
}

@Namespace("a")
class NamespaceNamedParameterNoAnnotation implements Name<String> {
}

@Namespace("a")
@NamedParameter(doc = "b")
class NamespaceNamedParameterAnnotation implements Name<String> {
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

@Namespace("java")
class NamespacePointsToPackage {
}

@Namespace("X")
class OverlappingNamespaces {
  class A {
  }
}

@Namespace("X.A")
class OverlappingNamespaces2 {
}

@Namespace("X.A")
class OverlappingNamespaces3 {
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
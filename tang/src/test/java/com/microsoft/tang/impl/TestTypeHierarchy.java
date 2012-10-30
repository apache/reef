package com.microsoft.tang.impl;

import javax.inject.Inject;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.tang.impl.TypeHierarchy;
import com.microsoft.tang.impl.TypeHierarchy.ClassNode;
import com.microsoft.tang.impl.TypeHierarchy.ConstructorDef;
import com.microsoft.tang.impl.TypeHierarchy.NamedParameterNode;
import com.microsoft.tang.impl.TypeHierarchy.NamespaceNode;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Namespace;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.NameResolutionException;

public class TestTypeHierarchy {
    TypeHierarchy ns;
    @Before
    public void setUp() throws Exception {
        ns = new TypeHierarchy();
    }
    @After
    public void tearDown() throws Exception {
        ns = null;
    }
    @Test
    public void testJavaString() throws NameResolutionException {
        ns.register(String.class);
        Assert.assertNotNull(ns.getNode("java"));
        Assert.assertNotNull(ns.getNode("java.lang"));
        Assert.assertNotNull(ns.getNode("java.lang.String"));
        Assert.assertNotNull(ns.getNode(String.class));
        try {
          ns.getNode("com.microsoft");
          Assert.fail("Didn't get expected exception");
        } catch(NameResolutionException e) {
          
        }
        try{
          ns.getNode(this.getClass());
          Assert.fail("Didn't get expected exception");
        } catch(NameResolutionException e) {
          
        }
    }
    @Test
    public void testSimpleConstructors() throws NameResolutionException {
        ns.register(SimpleConstructors.class);
        Assert.assertNotNull(ns.getNode(SimpleConstructors.class.getName()));
        ClassNode<?> cls = (ClassNode<?>)ns.getNode(SimpleConstructors.class);
        Assert.assertTrue(cls.children.size() == 0);
        ConstructorDef<?> def[] = cls.injectableConstructors;
        Assert.assertEquals(3, def.length);
        
    }
    @Test
    public void testNamedParameterConstructors() {
        ns.register(NamedParameterConstructors.class);
    }
    @Test(expected = UnsupportedOperationException.class)
    public void testArray() {
        ns.register(new String[0].getClass());
    }
    @Test
    public void testMetadata() throws NameResolutionException {
        ns.register(Metadata.class);
        Assert.assertNotNull(ns.getNode(Metadata.class));
        Assert.assertFalse(ns.getNode("foo.bar") instanceof NamedParameterNode);
        Assert.assertTrue(ns.getNode("foo.bar.Quuz") instanceof NamedParameterNode);
        Assert.assertTrue(((ClassNode<?>)ns.getNode(Metadata.class)).getIsPrefixTarget());
    }
    @Test(expected = IllegalArgumentException.class)
    public void testRepeatConstructorArg() {
        ns.register(RepeatConstructorArg.class);
    }
    @Test
    public void testResolveDependencies() throws NameResolutionException {
        ns.register(SimpleConstructors.class);
        Assert.assertNotNull(ns.getNode(String.class));
    }
    @Test
    public void testDocumentedLocalNamedParameter() {
      ns.register(DocumentedLocalNamedParameter.class);
    }
    // TODO need better exception type for conflicting named parameters
    @Test(expected = IllegalArgumentException.class)
    public void testConflictingLocalNamedParameter() {
      ns.register(ConflictingLocalNamedParameter.class);
    }
    @Test
    public void testInconvenientNamespaceRegistrationOrder() throws NameResolutionException {
      ns.register(InconvenientNamespaceRegistrationOrder1.class);
      Assert.assertTrue(ns.getNode("a.b") instanceof NamespaceNode);
      ns.register(InconvenientNamespaceRegistrationOrder2.class);
      Assert.assertTrue(ns.getNode("a.b") instanceof NamespaceNode);
      Assert.assertTrue(ns.getNode("a") instanceof NamespaceNode);
      Assert.assertTrue(ns.getNode("a.B") instanceof NamedParameterNode);
      Assert.assertTrue(ns.getNode("a.b.C") instanceof NamedParameterNode);
    }
}

class SimpleConstructors{
  @Inject
  public SimpleConstructors() { }
  @Inject
  public SimpleConstructors(int x) { }
  public SimpleConstructors(String x) { }
  @Inject
  public SimpleConstructors(int x, String y) { }
}
class NamedParameterConstructors {
  @NamedParameter()
  class X implements Name<String> {};
  @Inject
  public NamedParameterConstructors(String x, @Parameter(X.class) String y) { }
}
@Namespace("foo.bar")
class Metadata { 
  @NamedParameter(doc = "a baz", default_value="woo")
  final class Baz implements Name<String> {};
  @NamedParameter(doc = "a bar", default_value="i-beam")
  final class Bar implements Name<String> {};
  @NamedParameter(doc = "???")
  final class Quuz implements Name<String>{};
}
class RepeatConstructorArg {
  public @Inject RepeatConstructorArg(int x, int y) {}
}
@Namespace("bar")
class DocumentedLocalNamedParameter {
  @NamedParameter(doc="doc stuff", default_value="some value")
  final class Foo implements Name<String> {}
  @Inject
  public DocumentedLocalNamedParameter(@Parameter(Foo.class) String s) {}
}
@Namespace("baz")
class ConflictingLocalNamedParameter {
  @NamedParameter(doc="doc.stuff", default_value="1")
  final class Foo implements Name<Integer> {}
  @Inject
  public ConflictingLocalNamedParameter(@Parameter(Foo.class) String s) {}
}
@Namespace("a.b")
class InconvenientNamespaceRegistrationOrder1 {
  @NamedParameter()
  final class C implements Name<String> {}
}
@Namespace("a")
class InconvenientNamespaceRegistrationOrder2 {
  @NamedParameter()
  final class B implements Name<String> {}
}

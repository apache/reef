package com.microsoft.inject;

import javax.inject.Inject;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.inject.TypeHierarchy.ClassNode;
import com.microsoft.inject.TypeHierarchy.ConstructorDef;
import com.microsoft.inject.TypeHierarchy.NamedParameterNode;
import com.microsoft.inject.annotations.Name;
import com.microsoft.inject.annotations.Namespace;
import com.microsoft.inject.annotations.NamedParameter;
import com.microsoft.inject.annotations.Parameter;
import com.microsoft.inject.exceptions.NameResolutionException;

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
        ns.registerClass(String.class);
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
        ns.registerClass(SimpleConstructors.class);
        Assert.assertNotNull(ns.getNode(SimpleConstructors.class.getName()));
        ClassNode cls = (ClassNode)ns.getNode(SimpleConstructors.class);
        Assert.assertTrue(cls.children.size() == 0);
        ConstructorDef def[] = cls.injectableConstructors;
        Assert.assertEquals(3, def.length);
        
    }
    @Test
    public void testNamedParameterConstructors() {
        ns.registerClass(NamedParameterConstructors.class);
    }
    @Test(expected = UnsupportedOperationException.class)
    public void testArray() {
        ns.registerClass(new String[0].getClass());
    }
    @Test
    public void testMetadata() throws NameResolutionException {
        ns.registerClass(Metadata.class);
        Assert.assertNotNull(ns.getNode(Metadata.class));
        Assert.assertFalse(ns.getNode("foo.bar") instanceof NamedParameterNode);
        Assert.assertTrue(ns.getNode("foo.bar.Quuz") instanceof ClassNode);
        Assert.assertTrue(((ClassNode)ns.getNode(Metadata.class)).isPrefixTarget);
    }
    @Test(expected = IllegalArgumentException.class)
    public void testRepeatConstructorArg() {
        ns.registerClass(RepeatConstructorArg.class);
    }
    @Test
    public void testResolveDependencies() throws NameResolutionException {
        ns.registerClass(SimpleConstructors.class);
        for(Class<?> c : ns.findUnresolvedClasses()) {
            ns.registerClass(c);
        }
        Assert.assertNotNull(ns.getNode(String.class));
    }
    @Test
    public void testDocumentedLocalNamedParameter() {
      ns.registerClass(DocumentedLocalNamedParameter.class);
    }
    // TODO need better exception type for conflicting named parameters
    @Test(expected = IllegalArgumentException.class)
    public void testConflictingLocalNamedParameter() {
      ns.registerClass(ConflictingLocalNamedParameter.class);
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
  class X implements Name {};
  @Inject
  public NamedParameterConstructors(String x, @Parameter(X.class) String y) { }
}
@Namespace("foo.bar")
class Metadata { 
  @NamedParameter(doc = "a baz", default_value="woo")
  final class Baz {};
  @NamedParameter(doc = "a bar", default_value="i-beam")
  final class Bar {};
  @NamedParameter(doc = "???")
  final class Quuz{};
}
class RepeatConstructorArg {
  public @Inject RepeatConstructorArg(int x, int y) {}
}
@Namespace("bar")
class DocumentedLocalNamedParameter {
  @NamedParameter(doc="doc stuff", default_value="some value")
  final class Foo implements Name {}
  @Inject
  public DocumentedLocalNamedParameter(@Parameter(Foo.class) String s) {}
}
@Namespace("baz")
class ConflictingLocalNamedParameter {
  @NamedParameter(type=int.class, doc="doc.stuff", default_value="1")
  final class Foo implements Name {}
  @Inject
  public ConflictingLocalNamedParameter(@Parameter(Foo.class) String s) {}
}

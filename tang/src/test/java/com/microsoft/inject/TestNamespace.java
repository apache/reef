package com.microsoft.inject;

import javax.inject.Inject;
import javax.inject.Named;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.inject.Namespace.ClassNode;
import com.microsoft.inject.Namespace.ConstructorDef;
import com.microsoft.inject.Namespace.NamedParameterNode;
import com.microsoft.inject.exceptions.NameResolutionException;

public class TestNamespace {
    Namespace ns;
    @Before
    public void setUp() throws Exception {
        ns = new Namespace();
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
    private static class SimpleConstructors{
        @Inject
        public SimpleConstructors() { }
        @Inject
        public SimpleConstructors(int x) { }
        @SuppressWarnings("unused")
        public SimpleConstructors(String x) { }
        @Inject
        public SimpleConstructors(int x, String y) { }
    }
    @Test
    public void testSimpleConstructors() throws NameResolutionException {
        ns.registerClass(SimpleConstructors.class);
        Assert.assertNotNull(ns.getNode(SimpleConstructors.class.getName()));
        ClassNode cls = (ClassNode)ns.getNode(SimpleConstructors.class);
        Assert.assertTrue(cls.children.size() == 2);
        ConstructorDef def[] = cls.injectableConstructors;
        Assert.assertEquals(3, def.length);
        
    }
    static class NamedParameterConstructors {
        @Inject
        public NamedParameterConstructors(String x, @Named(value="x") String y) { }
    }
    @Test
    public void testNamedParameterConstructors() {
        ns.registerClass(NamedParameterConstructors.class);
    }
    static class InvalidNamedParameterConstructors {
        @Inject // This should fail since @Named needs to specify a name to help us.
        public InvalidNamedParameterConstructors(String x, @Named String y) { }
    }
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNamedParameterConstructors() {
        ns.registerClass(InvalidNamedParameterConstructors.class);
    }
    @Test(expected = UnsupportedOperationException.class)
    public void testArray() {
        ns.registerClass(new String[0].getClass());
    }
    private @ConfigurationMetadata(name = "foo.bar",
       params = {
            @NamedParameter(value="baz", doc="a baz", default_value="woo"),
            @NamedParameter(value="bar", doc="a bar", default_value="i-beam"),
            @NamedParameter(value="quuz", doc="???")
        })
    static class Metadata { }
    @Test
    public void testMetadata() throws NameResolutionException {
        ns.registerClass(Metadata.class);
        Assert.assertNotNull(ns.getNode(Metadata.class));
        Assert.assertFalse(ns.getNode("foo.bar") instanceof NamedParameterNode);
        Assert.assertTrue(ns.getNode("foo.bar.quuz") instanceof NamedParameterNode);
        Assert.assertTrue(((ClassNode)ns.getNode(Metadata.class)).isPrefixTarget);
    }
    class RepeatConstructorArg {
        public @Inject RepeatConstructorArg(int x, int y) {}
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
    @ConfigurationMetadata(name = "bar",
        params = { @NamedParameter(value="foo", doc="doc stuff", default_value="some value") } )
    static class DocumentedLocalNamedParameter {
      @Inject
      public DocumentedLocalNamedParameter(@Named("foo") String s) {}
    }
    @Test
    public void testDocumentedLocalNamedParameter() {
      ns.registerClass(DocumentedLocalNamedParameter.class);
    }
    @ConfigurationMetadata(name = "bar",
        params = { @NamedParameter(value="foo", type="int", doc="doc stuff", default_value="some value") } )
    static class ConflictingLocalNamedParameter {
      @Inject
      public ConflictingLocalNamedParameter(@Named("foo") String s) {}
    }
    // TODO need better exception type for conflicting named parameters
    @Test(expected = IllegalArgumentException.class)
    public void testConflictingLocalNamedParameter() {
      ns.registerClass(ConflictingLocalNamedParameter.class);
    }
}

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
    public void testJavaString() {
        ns.registerClass(String.class);
        Assert.assertNotNull(ns.getNode("java"));
        Assert.assertNotNull(ns.getNode("java.lang"));
        Assert.assertNotNull(ns.getNode("java.lang.String"));
        Assert.assertNotNull(ns.getNode(String.class));
        Assert.assertNull(ns.getNode("com.microsoft"));
        Assert.assertNull(ns.getNode(this.getClass()));
    }
    private class SimpleConstructors{
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
    public void testSimpleConstructors() {
        ns.registerClass(SimpleConstructors.class);
        Assert.assertNotNull(ns.getNode(SimpleConstructors.class.getName()));
        ClassNode cls = (ClassNode)ns.getNode(SimpleConstructors.class);
        Assert.assertTrue(cls.children == null || cls.children.size() == 0);
        ConstructorDef def[] = cls.injectableConstructors;
        Assert.assertEquals(3, def.length);
        
    }
    class NamedParameterConstructors {
        @Inject
        public NamedParameterConstructors(String x, @Named(value="x") String y) { }
    }
    @Test
    public void testNamedParameterConstructors() {
        ns.registerClass(NamedParameterConstructors.class);
    }
    class InvalidNamedParameterConstructors {
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
    class Metadata { }
    @Test
    public void testMetadata() {
        ns.registerClass(Metadata.class);
        Assert.assertNotNull(ns.getNode(Metadata.class));
        Assert.assertNotNull(ns.getNode("foo.bar"));
        Assert.assertFalse(ns.getNode("foo.bar") instanceof NamedParameterNode);
        Assert.assertNotNull(ns.getNode("foo.bar.quuz"));
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
    public void testResolveDependencies() {
        ns.registerClass(SimpleConstructors.class);
        for(Class<?> c : ns.findUnresolvedClasses()) {
            ns.registerClass(c);
        }
        Assert.assertNotNull(ns.getNode(String.class));
    }
}

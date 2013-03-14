package com.microsoft.tang.implementation.java;

import javax.inject.Inject;

import org.junit.Assert;
import org.junit.Test;

import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ParameterParser;
import com.microsoft.tang.util.ReflectionUtilities;

public class TestParameterParser {
  @Test
  public void testParameterParser() throws BindException {
    ParameterParser p = new ParameterParser();
    p.addParser(FooParser.class);
    Foo f = p.parse(Foo.class, "woot");
    Assert.assertEquals(f.s, "woot");
  }
  @Test(expected=UnsupportedOperationException.class)
  public void testUnregisteredParameterParser() throws BindException {
    ParameterParser p = new ParameterParser();
    //p.addParser(FooParser.class);
    Foo f = p.parse(Foo.class, "woot");
    Assert.assertEquals(f.s, "woot");
  }
  @Test
  public void testReturnSubclass() throws BindException {
    ParameterParser p = new ParameterParser();
    p.addParser(BarParser.class);
    Bar f = (Bar)p.parse(Foo.class, "woot");
    Assert.assertEquals(f.s, "woot");
    
  }
  @Test
  public void testGoodMerge() throws BindException {
    ParameterParser old = new ParameterParser();
    old.addParser(BarParser.class);
    ParameterParser nw = new ParameterParser();
    nw.mergeIn(old);
    nw.parse(Foo.class, "woot");
  }
  @Test
  public void testGoodMerge2() throws BindException {
    ParameterParser old = new ParameterParser();
    old.addParser(BarParser.class);
    ParameterParser nw = new ParameterParser();
    nw.addParser(BarParser.class);
    nw.mergeIn(old);
    nw.parse(Foo.class, "woot");
  }
  @Test(expected=IllegalArgumentException.class)
  public void testBadMerge() throws BindException {
    ParameterParser old = new ParameterParser();
    old.addParser(BarParser.class);
    ParameterParser nw = new ParameterParser();
    nw.addParser(FooParser.class);
    nw.mergeIn(old);
    nw.parse(Foo.class, "woot");
  }
  @Test
  public void testEndToEnd() throws BindException, InjectionException {
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindParser(BarParser.class);
    cb.bindNamedParameter(SomeNamedFoo.class, "hdfs://woot");
    ILikeBars ilb = tang.newInjector(cb.build()).getInstance(ILikeBars.class);
    Assert.assertNotNull(ilb);
  }
  @Test
  public void testDelegatingParser() throws BindException, InjectionException, ClassNotFoundException {
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    cb.bindParser(ParseTypeA.class, TypeParser.class);
    cb.bindParser(ParseTypeB.class, TypeParser.class);
    
    cb.bindParser(ParseableType.class, TypeParser.class);
    
    JavaConfigurationBuilder cb2 = tang.newConfigurationBuilder(cb.build());
    
    cb2.bind(ReflectionUtilities.getFullName(ParseName.class), "a");

    ParseableType t = tang.newInjector(cb2.build()).getNamedInstance(ParseName.class);
    Assert.assertTrue(t instanceof ParseTypeA);
    
    cb2 = tang.newConfigurationBuilder(cb.build());
    
    cb2.bind(ReflectionUtilities.getFullName(ParseNameB.class), "b");
    cb2.bind(ReflectionUtilities.getFullName(ParseNameA.class), "a");
    tang.newInjector(cb2.build()).getInstance(NeedsA.class);    
    tang.newInjector(cb2.build()).getInstance(NeedsB.class);
    
  }
  private static class FooParser implements ExternalConstructor<Foo> {
    private final Foo foo;
    @Inject
    public FooParser(String s) {
      this.foo = new Foo(s);
    }
    @Override
    public Foo newInstance() {
      return foo;
    }
  }
  private static class BarParser implements ExternalConstructor<Foo> {
    private final Bar bar;
    @Inject
    public BarParser(String s) {
      this.bar = new Bar(s);
    }
    @Override
    public Bar newInstance() {
      return bar;
    }
  }
  private static class Foo {
    public final String s;
    public Foo(String s) { this.s = s; }
  }
  private static class Bar extends Foo{
    public Bar(String s) { super(s); }
  }
  @NamedParameter
  private static class SomeNamedFoo implements Name<Foo> {}
  private static class ILikeBars {
    @Inject ILikeBars(@Parameter(SomeNamedFoo.class) Foo bar) {
      Bar b = (Bar) bar;
      Assert.assertEquals(b.s, "hdfs://woot");
    }
  }
  private static class ParseableType {
  }
  private static class ParseTypeA extends ParseableType {

  }
  private static class ParseTypeB extends ParseableType {
    
  }
  private static class TypeParser implements ExternalConstructor<ParseableType> {
    ParseableType instance;
    @Inject
    public TypeParser(String s) {
      if(s.equals("a")) { instance = new ParseTypeA(); }
      if(s.equals("b")) { instance = new ParseTypeB(); }
      
    }
    @Override
    public ParseableType newInstance() {
      return instance;
    }
  }
  @NamedParameter()
  private static class ParseName implements Name<ParseableType> {
    
  }
  @NamedParameter()
  private static class ParseNameA implements Name<ParseableType> {
    
  }
  @NamedParameter()
  private static class ParseNameB implements Name<ParseTypeB> {
    
  }
  private static class NeedsA {
    @Inject public NeedsA(@Parameter(ParseNameA.class) ParseableType a) {
      Assert.assertTrue(a instanceof ParseTypeA);
    }
  }
  private static class NeedsB {
    @Inject public NeedsB(@Parameter(ParseNameB.class) ParseTypeB b) {
      Assert.assertTrue(b instanceof ParseTypeB);
    }
  }
}

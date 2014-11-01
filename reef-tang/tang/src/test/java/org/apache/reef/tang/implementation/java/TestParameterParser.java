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
package org.apache.reef.tang.implementation.java;

import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ParameterParser;
import org.apache.reef.tang.util.ReflectionUtilities;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.inject.Inject;

public class TestParameterParser {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testParameterParser() throws BindException {
    ParameterParser p = new ParameterParser();
    p.addParser(FooParser.class);
    Foo f = p.parse(Foo.class, "woot");
    Assert.assertEquals(f.s, "woot");
  }

  @Test
  public void testUnregisteredParameterParser() throws BindException {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Don't know how to parse a org.apache.reef.tang.implementation.java.TestParameterParser$Foo");
    ParameterParser p = new ParameterParser();
    //p.addParser(FooParser.class);
    Foo f = p.parse(Foo.class, "woot");
    Assert.assertEquals(f.s, "woot");
  }

  @Test
  public void testReturnSubclass() throws BindException {
    ParameterParser p = new ParameterParser();
    p.addParser(BarParser.class);
    Bar f = (Bar) p.parse(Foo.class, "woot");
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

  @Test
  public void testBadMerge() throws BindException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Conflict detected when merging parameter parsers! To parse org.apache.reef.tang.implementation.java.TestParameterParser$Foo I have a: org.apache.reef.tang.implementation.java.TestParameterParser$FooParser the other instance has a: org.apache.reef.tang.implementation.java.TestParameterParser$BarParser");
    ParameterParser old = new ParameterParser();
    old.addParser(BarParser.class);
    ParameterParser nw = new ParameterParser();
    nw.addParser(FooParser.class);
    nw.mergeIn(old);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testEndToEnd() throws BindException, InjectionException {
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder(BarParser.class);
    cb.bindNamedParameter(SomeNamedFoo.class, "hdfs://woot");
    ILikeBars ilb = tang.newInjector(cb.build()).getInstance(ILikeBars.class);
    Assert.assertNotNull(ilb);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDelegatingParser() throws BindException, InjectionException, ClassNotFoundException {
    Tang tang = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = tang.newConfigurationBuilder(TypeParser.class);

    JavaConfigurationBuilder cb2 = tang.newConfigurationBuilder(cb.build());

    cb2.bind(ReflectionUtilities.getFullName(ParseName.class), "a");

    ParseableType t = tang.newInjector(cb2.build()).getNamedInstance(ParseName.class);
    Assert.assertTrue(t instanceof ParseTypeA);

    cb2 = tang.newConfigurationBuilder(cb.build());

    cb2.bind(ReflectionUtilities.getFullName(ParseNameB.class), "b");
    cb2.bindNamedParameter(ParseNameA.class, "a");
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

    public Foo(String s) {
      this.s = s;
    }
  }

  private static class Bar extends Foo {
    public Bar(String s) {
      super(s);
    }
  }

  @NamedParameter
  private static class SomeNamedFoo implements Name<Foo> {
  }

  private static class ILikeBars {
    @Inject
    ILikeBars(@Parameter(SomeNamedFoo.class) Foo bar) {
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
      if (s.equals("a")) {
        instance = new ParseTypeA();
      }
      if (s.equals("b")) {
        instance = new ParseTypeB();
      }

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
    @Inject
    public NeedsA(@Parameter(ParseNameA.class) ParseableType a) {
      Assert.assertTrue(a instanceof ParseTypeA);
    }
  }

  private static class NeedsB {
    @Inject
    public NeedsB(@Parameter(ParseNameB.class) ParseTypeB b) {
      Assert.assertTrue(b instanceof ParseTypeB);
    }
  }
}

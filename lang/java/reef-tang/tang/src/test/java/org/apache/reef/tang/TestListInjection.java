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
package org.apache.reef.tang;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.tang.types.ClassNode;
import org.apache.reef.tang.types.NamedParameterNode;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for list injection in Tang.
 */
public class TestListInjection {

  /**
   * Test code for injecting default list with string elements.
   *
   * @throws InjectionException
   */
  @Test
  public void testStringInjectDefault() throws InjectionException {
    final List<String> actual = Tang.Factory.getTang().newInjector().getInstance(StringClass.class).getStringList();
    final List<String> expected = new ArrayList<>();
    expected.add("bye");
    expected.add("hello");
    expected.add("hi");
    Assert.assertEquals(expected, actual);
  }

  /**
   * Test code for injecting default list with non-string values.
   *
   * @throws InjectionException
   */
  @Test
  public void testIntegerInjectDefault() throws InjectionException {
    final List<Integer> actual = Tang.Factory.getTang().newInjector().getInstance(IntegerClass.class).getIntegerList();
    final List<Integer> expected = new ArrayList<>();
    expected.add(1);
    expected.add(2);
    expected.add(3);
    Assert.assertEquals(expected, actual);
  }

  /**
   * Test code for injecting default list with implementations.
   *
   * @throws InjectionException
   */
  @Test
  public void testObjectInjectDefault() throws InjectionException {
    final Integer integer = 1;
    final Float ffloat = 1.001f;

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(Integer.class, integer);
    injector.bindVolatileInstance(Float.class, ffloat);
    final List<Number> actual = injector.getInstance(NumberClass.class).getNumberList();
    final List<Number> expected = new ArrayList<>();
    expected.add(integer);
    expected.add(ffloat);
    Assert.assertEquals(expected, actual);
  }

  /**
   * Test code for injecting list with String elements.
   *
   * @throws InjectionException
   */
  @Test
  public void testStringInjectBound() throws InjectionException {
    final List<String> injected = new ArrayList<>();
    injected.add("hi");
    injected.add("hello");
    injected.add("bye");
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindList(StringList.class, injected);
    final List<String> actual = 
        Tang.Factory.getTang().newInjector(cb.build()).getInstance(StringClass.class).getStringList();
    final List<String> expected = new ArrayList<>();
    expected.add("hi");
    expected.add("hello");
    expected.add("bye");
    Assert.assertEquals(expected, actual);
  }

  /**
   * Test code for injecting list with parsable non-string values.
   *
   * @throws InjectionException
   */
  @Test
  public void testIntegerInjectBound() throws InjectionException {
    final List<String> injected = new ArrayList<>();
    injected.add("1");
    injected.add("2");
    injected.add("3");
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindList(IntegerList.class, injected);

    final List<Integer> actual =
        Tang.Factory.getTang().newInjector(cb.build()).getInstance(IntegerClass.class).getIntegerList();
    final List<Integer> expected = new ArrayList<>();
    expected.add(1);
    expected.add(2);
    expected.add(3);
    Assert.assertEquals(expected, actual);
  }

  /**
   * Test code for injecting list with implementations.
   *
   * @throws InjectionException
   */
  @Test
  public void testObjectInjectBound() throws InjectionException {
    final Integer integer = 1;
    final Float ffloat = 1.001f;

    // Inject implementations via class object
    final List<Class> injected1 = new ArrayList<>();
    injected1.add(Integer.class);
    injected1.add(Float.class);
    final JavaConfigurationBuilder cb1 = Tang.Factory.getTang().newConfigurationBuilder();
    cb1.bindList(NumberList.class, injected1);
    final Injector injector1 = Tang.Factory.getTang().newInjector(cb1.build());
    injector1.bindVolatileInstance(Integer.class, integer);
    injector1.bindVolatileInstance(Float.class, ffloat);
    final List<Number> actual1 = injector1.getInstance(NumberClass.class).getNumberList();

    // Inject implementations via class name
    final List<String> injected2 = new ArrayList<>();
    injected2.add("java.lang.Integer");
    injected2.add("java.lang.Float");
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindList(NumberList.class, injected2);
    final Injector injector2 = Tang.Factory.getTang().newInjector(cb.build());
    injector2.bindVolatileInstance(Integer.class, integer);
    injector2.bindVolatileInstance(Float.class, ffloat);
    final List<Number> actual2 = injector2.getInstance(NumberClass.class).getNumberList();

    final List<Number> expected = new ArrayList<>();
    expected.add(integer);
    expected.add(ffloat);
    Assert.assertEquals(expected, actual1);
    Assert.assertEquals(expected, actual2);
  }

  // TODO: Make tests for list serialization/deserialization after implementing those features.

  /**
   * Test code for Tang selectivity.
   *
   * @throws InjectionException
   */
  @Test
  public void testInjectSelectiveConstructor() throws InjectionException {
    // Test injection without list binding
    final List<String> actual1 =
        Tang.Factory.getTang().newInjector().getInstance(SelectiveConsructorClass.class).getList();
    final List<String> expected1 = new ArrayList<>();
    Assert.assertEquals(expected1, actual1);
    // Test injection with list binding
    final List<String> injected = new ArrayList<>();
    injected.add("hi");
    injected.add("hello");
    injected.add("bye");
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindList(SelectiveInjectTestList.class, injected);
    final List<String> actual2 = 
        Tang.Factory.getTang().newInjector(cb.build()).getInstance(SelectiveConsructorClass.class).getList();
    final List<String> expected2 = new ArrayList<>();
    expected2.add("hi");
    expected2.add("hello");
    expected2.add("bye");
    Assert.assertEquals(expected2, actual2);
  }

  /**
   * Test code for injecting list of strings with ConfigurationBuilder.
   *
   * @throws InjectionException
   */
  @Test
  public void testStringInjectConfigurationBuilder() throws InjectionException {
    final JavaClassHierarchy namespace = Tang.Factory.getTang().getDefaultClassHierarchy();
    final NamedParameterNode<List<String>> np = (NamedParameterNode) namespace.getNode(StringList.class);
    final List<String> injected = new ArrayList<>();
    injected.add("hi");
    injected.add("hello");
    injected.add("bye");

    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindList(np, injected);
    final List<String> actual = 
        Tang.Factory.getTang().newInjector(cb.build()).getInstance(StringClass.class).getStringList();
    final List<String> expected = new ArrayList<>();
    expected.add("hi");
    expected.add("hello");
    expected.add("bye");
    Assert.assertEquals(expected, actual);
  }

  /**
   * Test code for injecting list of implementations with ConfigurationBuilder.
   *
   * @throws InjectionException
   */
  @Test
  public void testObjectInjectConfigurationBuilder() throws InjectionException {
    final Integer integer = 1;
    final Float ffloat = 1.001f;

    final JavaClassHierarchy namespace = Tang.Factory.getTang().getDefaultClassHierarchy();
    final NamedParameterNode<List<Class>> np = (NamedParameterNode) namespace.getNode(NumberList.class);
    final List<ClassNode> injected = new ArrayList<>();
    injected.add((ClassNode) namespace.getNode(Integer.class));
    injected.add((ClassNode) namespace.getNode(Float.class));

    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindList(np, injected);

    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    injector.bindVolatileInstance(Integer.class, integer);
    injector.bindVolatileInstance(Float.class, ffloat);
    final List<Number> actual = injector.getInstance(NumberClass.class).getNumberList();
    final List<Number> expected = new ArrayList<>();
    expected.add(integer);
    expected.add(ffloat);
    Assert.assertEquals(expected, actual);
  }

  /**
   * Test code for injectiong list with ConfigurationModule.
   *
   * @throws InjectionException
   */
  @Test
  public void testInjectConfigurationModule() throws InjectionException {
    final List<String> injected = new ArrayList<>();
    injected.add("hi");
    injected.add("hello");
    injected.add("bye");
    final Configuration conf = StringClassConfiguration.CONF
        .set(StringClassConfiguration.STRING_LIST, injected)
        .build();
    final List<String> actual = Tang.Factory.getTang().newInjector(conf).getInstance(StringClass.class).getStringList();
    final List<String> expected = new ArrayList<>();
    expected.add("hi");
    expected.add("hello");
    expected.add("bye");
    Assert.assertEquals(expected, actual);
  }

  // ConfigurationModuleBuilder for StringClass
  public static class StringClassConfiguration extends ConfigurationModuleBuilder {
    public static final RequiredParameter<List> STRING_LIST = new RequiredParameter<>();

    public static final ConfigurationModule CONF = new StringClassConfiguration()
        .bindList(StringList.class, StringClassConfiguration.STRING_LIST)
        .build();
  }
}

@NamedParameter(default_values = {"bye", "hello", "hi"})
class StringList implements Name<List<String>> {
}

@NamedParameter(default_values = {"1", "2", "3"})
class IntegerList implements Name<List<Integer>> {
}

@NamedParameter(default_values = {"java.lang.Integer", "java.lang.Float"})
class NumberList implements Name<List<Number>> {
}

@NamedParameter
class SelectiveInjectTestList implements Name<List<String>> {
}

class SelectiveConsructorClass {
  private final List<String> list;

  public List<String> getList() {
    return list;
  }

  @Inject
  SelectiveConsructorClass() {
    list = new ArrayList<>();
  }

  @Inject
  SelectiveConsructorClass(@Parameter(SelectiveInjectTestList.class) final List<String> list) {
    this.list = list;
  }

}

class StringClass {
  private final List<String> stringList;

  public List<String> getStringList() {
    return stringList;
  }

  @Inject
  StringClass(@Parameter(StringList.class) final List<String> stringList) {
    this.stringList = stringList;
  }
}

class IntegerClass {
  private final List<Integer> integerList;

  public List<Integer> getIntegerList() {
    return integerList;
  }

  @Inject
  IntegerClass(@Parameter(IntegerList.class) final List<Integer> integerList) {
    this.integerList = integerList;
  }
}

class NumberClass {
  private final List<Number> numberList;

  public List<Number> getNumberList() {
    return numberList;
  }

  @Inject
  NumberClass(@Parameter(NumberList.class) final List<Number> numberList) {
    this.numberList = numberList;
  }
}

/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.tang;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;

import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.formats.ConfigurationSerializer;
import org.junit.Assert;

import org.junit.Test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

public class TestSetInjection {
  @Test
  public void testStringInjectDefault() throws InjectionException {
    Set<String> actual = Tang.Factory.getTang().newInjector().getInstance(Box.class).strings;

    Set<String> expected = new HashSet<>();
    expected.add("one");
    expected.add("two");
    expected.add("three");

    Assert.assertEquals(expected, actual);
  }
  @Test
  public void testObjectInjectDefault() throws InjectionException, BindException {
    Injector i = Tang.Factory.getTang().newInjector();
    i.bindVolatileInstance(Integer.class, 42);
    i.bindVolatileInstance(Float.class, 42.0001f);
    Set<Number> actual = i.getInstance(Pool.class).numbers;
    Set<Number> expected = new HashSet<>();
    expected.add(42);
    expected.add(42.0001f);
    Assert.assertEquals(expected, actual);
  }
  @Test
  public void testStringInjectBound() throws InjectionException, BindException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindSetEntry(SetOfStrings.class, "four");
    cb.bindSetEntry(SetOfStrings.class, "five");
    cb.bindSetEntry(SetOfStrings.class, "six");
    Set<String> actual = Tang.Factory.getTang().newInjector(cb.build()).getInstance(Box.class).strings;

    Set<String> expected = new HashSet<>();
    expected.add("four");
    expected.add("five");
    expected.add("six");

    Assert.assertEquals(expected, actual);
  }
  @Test
  public void testObjectInjectBound() throws InjectionException, BindException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindSetEntry(SetOfClasses.class, Short.class);
    cb.bindSetEntry(SetOfClasses.class, Float.class);
    
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    i.bindVolatileInstance(Short.class, (short)4);
    i.bindVolatileInstance(Float.class, 42.0001f);
    Set<Number> actual = i.getInstance(Pool.class).numbers;
    Set<Number> expected = new HashSet<>();
    expected.add((short)4);
    expected.add(42.0001f);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testStringInjectRoundTrip() throws InjectionException, BindException, IOException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindSetEntry(SetOfStrings.class, "four");
    cb.bindSetEntry(SetOfStrings.class, "five");
    cb.bindSetEntry(SetOfStrings.class, "six");

    ConfigurationSerializer serializer = new AvroConfigurationSerializer();

    String s = serializer.toString(cb.build());
    JavaConfigurationBuilder cb2 = Tang.Factory.getTang().newConfigurationBuilder();
    Configuration conf = serializer.fromString(s);
    cb2.addConfiguration(conf);

    Set<String> actual = Tang.Factory.getTang().newInjector(cb2.build()).getInstance(Box.class).strings;

    Set<String> expected = new HashSet<>();
    expected.add("four");
    expected.add("five");
    expected.add("six");

    Assert.assertEquals(expected, actual);
  }
  @Test
  public void testObjectInjectRoundTrip() throws InjectionException, BindException, IOException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindSetEntry(SetOfClasses.class, Short.class);
    cb.bindSetEntry(SetOfClasses.class, Float.class);

    ConfigurationSerializer serializer = new AvroConfigurationSerializer();

    String s = serializer.toString(cb.build());
    JavaConfigurationBuilder cb2 = Tang.Factory.getTang().newConfigurationBuilder();
    Configuration conf = serializer.fromString(s);
    cb2.addConfiguration(conf);
    
    Injector i = Tang.Factory.getTang().newInjector(cb2.build());
    i.bindVolatileInstance(Short.class, (short)4);
    i.bindVolatileInstance(Float.class, 42.0001f);
    Set<Number> actual = i.getInstance(Pool.class).numbers;
    Set<Number> expected = new HashSet<>();
    expected.add((short)4);
    expected.add(42.0001f);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDefaultAsClass() throws InjectionException, BindException {
    Injector i = Tang.Factory.getTang().newInjector();
    i.bindVolatileInstance(Integer.class, 1);
    i.bindVolatileInstance(Float.class, 2f);
    Set<Number> actual = i.getNamedInstance(SetOfClassesDefaultClass.class);
    Set<Number> expected = new HashSet<>();
    expected.add(1);
    Assert.assertEquals(expected, actual);
  }
  
}

@NamedParameter(default_values={"one","two","three"})
class SetOfStrings implements Name<Set<String>> { }

class Box {
  public final Set<String> strings;
  @Inject
  Box(@Parameter(SetOfStrings.class) Set<String> strings) {
    this.strings = strings;
  }
}

@NamedParameter(default_values={"java.lang.Integer","java.lang.Float"})
class SetOfClasses implements Name<Set<Number>> { }

class Pool {
  public final Set<Number> numbers;
  @Inject
  Pool(@Parameter(SetOfClasses.class) Set<Number> numbers) {
    this.numbers = numbers;
  }
}

@NamedParameter(default_class=Integer.class)
class SetOfClassesDefaultClass implements Name<Set<Number>> { }
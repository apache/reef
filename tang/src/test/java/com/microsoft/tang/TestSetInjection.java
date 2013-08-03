package com.microsoft.tang;

import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;

import junit.framework.Assert;

import org.junit.Test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

public class TestSetInjection {
  @Test
  public void testStringInject() throws InjectionException {
    Set<String> actual = Tang.Factory.getTang().newInjector().getInstance(Box.class).numbers;

    Set<String> expected = new HashSet<>();
    expected.add("one");
    expected.add("two");
    expected.add("three");

    Assert.assertEquals(expected, actual);
  }
  @Test
  public void testObjectInject() throws InjectionException, BindException {
    Injector i = Tang.Factory.getTang().newInjector();
    i.bindVolatileInstance(Integer.class, 42);
    i.bindVolatileInstance(Float.class, 42.0001f);
    Set<Number> actual = i.getInstance(Pool.class).numbers;
    Set<Number> expected = new HashSet<>();
    expected.add(42);
    expected.add(42.0001f);
    Assert.assertEquals(expected, actual);
  }
}

@NamedParameter(default_value="one,two,three")
class SetOfNumbers implements Name<Set<String>> { }

class Box {
  public final Set<String> numbers;
  @Inject
  Box(@Parameter(SetOfNumbers.class) Set<String> numbers) {
    this.numbers = numbers;
  }
}

@NamedParameter(default_value="java.lang.Integer,java.lang.Float")
class SetOfClasses implements Name<Set<Number>> { }

class Pool {
  public final Set<Number> numbers;
  @Inject
  Pool(@Parameter(SetOfClasses.class) Set<Number> numbers) {
    this.numbers = numbers;
  }
}
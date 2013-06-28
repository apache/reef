package com.microsoft.tang.formats;

import javax.inject.Inject;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationModule.OptionalParameter;
import com.microsoft.tang.formats.ConfigurationModule.RequiredImpl;

public class TestConfigurationModule {

  @Rule public ExpectedException thrown = ExpectedException.none();
  
  @Test
  public void smokeTest() throws BindException, InjectionException {
    Configuration c = MyConfigurationModule.CONF
        .set(MyConfigurationModule.THE_FOO, FooImpl.class)
        .set(MyConfigurationModule.FOO_NESS, ""+12)
        .build();
    Foo f = Tang.Factory.getTang().newInjector(c).getInstance(Foo.class);
    Assert.assertEquals(f.getFooness(), 12);
  }
  @Test
  public void omitOptionalTest() throws BindException, InjectionException {
    Configuration c = MyConfigurationModule.CONF
        .set(MyConfigurationModule.THE_FOO, FooImpl.class)
        .build();
    Foo f = Tang.Factory.getTang().newInjector(c).getInstance(Foo.class);
    Assert.assertEquals(f.getFooness(), 42);
  }
  @Test
  public void omitRequiredTest() throws BindException, InjectionException {
    thrown.expect(BindException.class);
    thrown.expectMessage("Attempt to build configuration before setting required option(s): { THE_FOO }");
    MyConfigurationModule.CONF
        .set(MyConfigurationModule.FOO_NESS, ""+12)
        .build();
  }
  @NamedParameter(default_value = "42")
  class Fooness implements Name<Integer> {} 
  static interface Foo { public int getFooness(); }
  static class FooImpl implements Foo {
    private final int fooness;
    @Inject
    FooImpl(@Parameter(Fooness.class) int fooness) { this.fooness = fooness;}
    
    public int getFooness() {
      return this.fooness;
    }
  }
    
  static class MyConfigurationModule {
    public static final RequiredImpl<Foo> THE_FOO = new RequiredImpl<>();
    public static final OptionalParameter<Integer> FOO_NESS = new OptionalParameter<>();
    public static final ConfigurationModule CONF = new ConfigurationModule() { }
      .bindImplementation(Foo.class, THE_FOO)
      .bindNamedParameter(Fooness.class, FOO_NESS);
    public static final ConfigurationModule BAD_CONF = new ConfigurationModule() { }
      .bindImplementation(Foo.class, THE_FOO);
  }
}


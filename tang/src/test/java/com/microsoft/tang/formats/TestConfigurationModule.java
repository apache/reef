package com.microsoft.tang.formats;

import javax.inject.Inject;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.exceptions.InjectionException;

/*
 * Define a configuration module that explains how Foo should be injected.
 * 
 * A configuration module is like a configuration builder, except that it
 * is not language independent (it should be defined in the same jar / whatever 
 * as the stuff it configures, and it has a concept of variables that can be
 * required or optional.
 * 
 * If you call build() without setting the required variables (or if the
 * configuration declares variables that it does not use), then it blows up
 * in your face.
 * 
 * Note that MyConfigurationModule does not actually subclass
 * ConfigurationModule.  Instead, it has a static final field that contains a
 * configuration module, and some other ones that define the parameters, and
 * their types.
 * 
 * There are some *ahem* non-idiomatic java things going on here.
 * 
 * Sorry about that; if you can find my java programming license, you can take it
 * away. :)
 *
 * First, you need the " = new RequiredImpl<>()" after each parameter.  This is
 * because you need to pass something into set (other than null).  References to
 * live objects happen to be unique, so that works.
 * 
 * Second, ConfigurationModule() is abstract, and all of its methods are defined
 * as final.  To instantiate it, you need to put the {}'s between the () and the
 * .bind stuff.  This is so I can call getClass().getEnclosingClass() in its
 * constructor, and discover all those juicy configuration parameters that
 * were assigned above it.  On the bright side, if you forget the {}'s you get
 * a compiler error.  It used to be that you'd get a cryptic NPE from the
 * classloader.  Also, note that adding methods to ConfigurationModule() won't
 * work.  The bind calls implement immutability by using a secret final clone
 * method called deepCopy() that strips your subclass off, and uses an anonomyous
 * inner class instead.
 * 
 * 
 */

final class MyBadConfigurationModule extends ConfigurationModuleBuilder {    

}

final class MyConfigurationModule extends ConfigurationModuleBuilder {    
  // Tell us what implementation you want, or else!!    
  public static final RequiredImpl<TestConfigurationModule.Foo> THE_FOO = new RequiredImpl<>();
  // If you want, you can change the fooness.
  public static final OptionalParameter<Integer> FOO_NESS = new OptionalParameter<>();
  
  public static final ConfigurationModule CONF = new MyConfigurationModule()

    // This binds the above to tang configuration stuff.  You can use parameters more than
    // once, but you'd better use them all at least once, or I'll throw exceptions at you.

    .bindImplementation(TestConfigurationModule.Foo.class, MyConfigurationModule.THE_FOO)
    .bindNamedParameter(TestConfigurationModule.Fooness.class, MyConfigurationModule.FOO_NESS)
    .build();
}
final class MyMissingBindConfigurationModule extends ConfigurationModuleBuilder {    
  // Tell us what implementation you want, or else!!    
  public static final RequiredImpl<TestConfigurationModule.Foo> THE_FOO = new RequiredImpl<>();
  // If you want, you can change the fooness.
  public static final OptionalParameter<Integer> FOO_NESS = new OptionalParameter<>();
  
  // This conf doesn't use FOO_NESS.  Expect trouble below
  public static final ConfigurationModule BAD_CONF = new MyMissingBindConfigurationModule()
    .bindImplementation(TestConfigurationModule.Foo.class, THE_FOO)
    .build();
}
public class TestConfigurationModule {
  /*
   *  Toy class hierarchy: FooImpl implements Foo, has a Fooness named
   *  parameter that defaults to 42.
   */
  
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

  static class FooAltImpl implements Foo {
    private final int fooness;
    @Inject
    FooAltImpl(@Parameter(Fooness.class) int fooness) { this.fooness = fooness;}
    
    public int getFooness() {
      return 7;
    }
  }

  @Rule public ExpectedException thrown = ExpectedException.none();
 
  @Test
  public void smokeTest() throws BindException, InjectionException {
    // Here we set some configuration values.  In true tang style,
    // you won't be able to set them more than once ConfigurationModule's
    // implementation is complete.
    Configuration c = MyConfigurationModule.CONF
        .set(MyConfigurationModule.THE_FOO, FooImpl.class)
        .set(MyConfigurationModule.FOO_NESS, ""+12)
        .build();
    Foo f = Tang.Factory.getTang().newInjector(c).getInstance(Foo.class);
    Assert.assertEquals(f.getFooness(), 12);
  }
  @Test
  public void omitOptionalTest() throws BindException, InjectionException {
    // Optional is optional.
    Configuration c = MyConfigurationModule.CONF
        .set(MyConfigurationModule.THE_FOO, FooImpl.class)
        .build();
    Foo f = Tang.Factory.getTang().newInjector(c).getInstance(Foo.class);
    Assert.assertEquals(f.getFooness(), 42);
  }
  @Test
  public void omitRequiredTest() throws Throwable {
    thrown.expect(BindException.class);
    thrown.expectMessage("Attempt to build configuration before setting required option(s): { THE_FOO }");
    try {
      MyConfigurationModule.CONF
        .set(MyConfigurationModule.FOO_NESS, ""+12)
        .build();
    } catch (ExceptionInInitializerError e) {
      throw e.getCause();
    }
  }
  @Test
  public void badConfTest() throws Throwable {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Found declared options that were not used in binds: { FOO_NESS }");
      try {
        // Java's classloader semantics cause it to load a class when executing the
        // first line that references the class in question.
        @SuppressWarnings("unused")
        Object o = MyMissingBindConfigurationModule.BAD_CONF;
      } catch (ExceptionInInitializerError e) {
        throw e.getCause();
      }
  }
  @Test
  public void nonExistentStringBindOK() throws BindException, InjectionException {
    new MyBadConfigurationModule().bindImplementation(Foo.class, "i.do.not.exist");
  }
  @Test
  public void nonExistentStringBindNotOK() throws BindException, InjectionException {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("ConfigurationModule refers to unknown class: i.do.not.exist");

    new MyBadConfigurationModule().bindImplementation(Foo.class, "i.do.not.exist").build();
  }

  public static final class MultiBindConfigurationModule extends ConfigurationModuleBuilder {    
    // Tell us what implementation you want, or else!!    
    public static final RequiredImpl<Foo> THE_FOO = new RequiredImpl<>();
    // If you want, you can change the fooness.
    public static final OptionalParameter<Integer> FOO_NESS = new OptionalParameter<>();
    
    public static final ConfigurationModule CONF = new MultiBindConfigurationModule()

      // This binds the above to tang configuration stuff.  You can use parameters more than
      // once, but you'd better use them all at least once, or I'll throw exceptions at you.

      .bindImplementation(Foo.class, THE_FOO)
      .bindImplementation(Object.class, THE_FOO)
      .bindNamedParameter(Fooness.class, FOO_NESS)
      .build();
    
  }
  @Test
  public void multiBindTest() throws BindException, InjectionException {
    // Here we set some configuration values.  In true tang style,
    // you won't be able to set them more than once ConfigurationModule's
    // implementation is complete.
    Configuration c = MultiBindConfigurationModule.CONF
        .set(MultiBindConfigurationModule.THE_FOO, FooImpl.class)
        .set(MultiBindConfigurationModule.FOO_NESS, ""+12)
        .build();
    Foo f = Tang.Factory.getTang().newInjector(c).getInstance(Foo.class);
    Foo g = (Foo)Tang.Factory.getTang().newInjector(c).getInstance(Object.class);
    Assert.assertEquals(f.getFooness(), 12);
    Assert.assertEquals(g.getFooness(), 12);
    Assert.assertFalse(f == g);
  }
  @Test
  public void foreignSetTest() throws Throwable {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Unknown Impl/Param when setting RequiredImpl.  Did you pass in a field from some other module?");
    try {
      // Pass in something from the wrong module, watch it fail.
      MultiBindConfigurationModule.CONF.set(MyConfigurationModule.THE_FOO, FooImpl.class);
    } catch (ExceptionInInitializerError e) {
      throw e.getCause();
    }
  }
  @Test
  public void foreignBindTest() throws Throwable {
    thrown.expect(ClassHierarchyException.class);
    thrown.expectMessage("Unknown Impl/Param when binding RequiredImpl.  Did you pass in a field from some other module?");
    try {
      // Pass in something from the wrong module, watch it fail.
      new MyConfigurationModule().bindImplementation(Object.class, MultiBindConfigurationModule.THE_FOO);
    } catch (ExceptionInInitializerError e) {
      throw e.getCause();
    }
  }
  @Test
  public void singletonTest() throws BindException, InjectionException {
    Configuration c = new MyConfigurationModule()
      .bindImplementation(Foo.class, MyConfigurationModule.THE_FOO)
      .bindSingleton(MyConfigurationModule.THE_FOO)
      .bindNamedParameter(Fooness.class, MyConfigurationModule.FOO_NESS)
      .build()
      .set(MyConfigurationModule.THE_FOO, FooImpl.class)
      .build();
    Injector i = Tang.Factory.getTang().newInjector(c);
    Assert.assertTrue(i.getInstance(Foo.class) == i.getInstance(Foo.class));
  }
  
  @Test
  public void immutablilityTest() throws BindException, InjectionException {
    // builder methods return copies; the original module is immutable
    ConfigurationModule builder1 = MyConfigurationModule.CONF
        .set(MyConfigurationModule.THE_FOO, FooImpl.class);
    Assert.assertFalse(builder1 == MyConfigurationModule.CONF );
    Configuration config1 = builder1.build();
  
    // reusable
    Configuration config2 = MyConfigurationModule.CONF
        .set(MyConfigurationModule.THE_FOO, FooAltImpl.class)
        .build();

    // instantiation of each just to be sure everything is fine in this situation
    Injector i1 = Tang.Factory.getTang().newInjector(config1);
    Injector i2 = Tang.Factory.getTang().newInjector(config2);
    Assert.assertEquals(42, i1.getInstance(Foo.class).getFooness());
    Assert.assertEquals(7, i2.getInstance(Foo.class).getFooness());
  }
}


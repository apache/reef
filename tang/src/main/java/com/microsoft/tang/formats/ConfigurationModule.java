package com.microsoft.tang.formats;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.formats.ConfigurationModuleBuilder.Impl;
import com.microsoft.tang.formats.ConfigurationModuleBuilder.OptionalParameter;
import com.microsoft.tang.formats.ConfigurationModuleBuilder.Param;
import com.microsoft.tang.util.MonotonicHashMap;
import com.microsoft.tang.util.MonotonicHashSet;
import com.microsoft.tang.util.ReflectionUtilities;

/**
 * Allows applications to bundle sets of configuration options together into 
 * discrete packages.  Unlike more conventional approaches,
 * ConfigurationModules store such information in static data structures that
 * can be statically discovered and sanity-checked. 
 * 
 * @see com.microsoft.tang.formats.TestConfigurationModule for more information and examples.
 *
 */
public class ConfigurationModule {
  private final ConfigurationModuleBuilder builder;
  // Set of required unset parameters. Must be empty before build.
  private final Set<Field> reqSet = new MonotonicHashSet<>();
  private final Map<Impl<?>, Class<?>> setImpls = new MonotonicHashMap<>();
  private final Map<Impl<?>, String> setLateImpls = new MonotonicHashMap<>();
  private final Map<Param<?>, String> setParams = new MonotonicHashMap<>();
  protected ConfigurationModule(ConfigurationModuleBuilder builder) {
    this.builder = builder.deepCopy();
  }
  private ConfigurationModule deepCopy() {
    ConfigurationModule cm = new ConfigurationModule(builder.deepCopy());
    cm.setImpls.putAll(setImpls);
    cm.setLateImpls.putAll(setLateImpls);
    cm.setParams.putAll(setParams);
    cm.reqSet.addAll(reqSet);
    return cm;
  }

  private final <T> void processSet(Object impl) {
    Field f = builder.map.get(impl);
    if (f == null) { /* throw */
      throw new ClassHierarchyException("Unknown Impl/Param when setting " + ReflectionUtilities.getSimpleName(impl.getClass()) + ".  Did you pass in a field from some other module?");
    }
    if(!reqSet.contains(impl)) { reqSet.add(f); }
  }

  public final <T> ConfigurationModule set(Impl<T> opt, Class<? extends T> impl) {
    ConfigurationModule c = deepCopy();
    c.processSet(opt);
    c.setImpls.put(opt, impl);
    return c;
  }

  public final <T> ConfigurationModule set(Impl<T> opt, String impl) {
    ConfigurationModule c = deepCopy();
    c.processSet(opt);
    c.setLateImpls.put(opt, impl);
    return c;
  }
  
  public final <T> ConfigurationModule set(Param<T> opt, String val) {
    ConfigurationModule c = deepCopy();
    c.processSet(opt);
    c.setParams.put(opt, val);
    return c;
  }
  
/*  public final InjectorModule buildVolatileInjector() throws ClassHierarchyException {
    ConfigurationModule c = deepCopy();

    return new InjectorModule(c);  // injector module will all bind volatiles over missing sets in c.
    
    // it basically just delegates to an unsafe version of build, gets an injector, and at build() bind volatiles into injector
  } */

  
  public Configuration build() throws BindException {
    ConfigurationModule c = deepCopy();
    
    if (!c.reqSet.containsAll(c.builder.reqDecl)) {
      Set<Field> missingSet = new MonotonicHashSet<>();
      for (Field f : c.builder.reqDecl) {
        if (!c.reqSet.contains(f)) {
          missingSet.add(f);
        }
      }
      throw new BindException(
          "Attempt to build configuration before setting required option(s): "
              + builder.toString(missingSet));
    }
  
    for (Class<?> clazz : c.builder.freeImpls.keySet()) {
      Impl<?> i = c.builder.freeImpls.get(clazz);
      if(c.setImpls.containsKey(i)) {
        c.builder.b.bind(clazz, c.setImpls.get(i));
      } else if(c.setLateImpls.containsKey(i)) {
        c.builder.b.bind(ReflectionUtilities.getFullName(clazz), c.setLateImpls.get(i));
      }
    }
    for (Class<? extends Name<?>> clazz : c.builder.freeParams.keySet()) {
      Param<?> p = c.builder.freeParams.get(clazz);
      String s = c.setParams.get(p);
      if(s != null) {
        c.builder.b.bindNamedParameter(clazz, s);
      } else {
        if(!(p instanceof OptionalParameter)) {
          throw new IllegalStateException();
        }
      }
    }
    return c.builder.b.build();

  }
}

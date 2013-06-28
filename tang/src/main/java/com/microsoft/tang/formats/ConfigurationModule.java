package com.microsoft.tang.formats;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.ClassHierarchyException;

public class ConfigurationModule {
  final JavaConfigurationBuilder b = Tang.Factory.getTang()
      .newConfigurationBuilder();
  
  public interface Impl<T> { };
  public interface Param<T> { };
  
  public static final class RequiredImpl<T> implements Impl<T> {
    public RequiredImpl() {
    }
  }

  public static final class OptionalImpl<T> implements Impl<T>{
    public OptionalImpl() {
    }
  }

  public static final class RequiredParameter<T> implements Param<T>{
    public RequiredParameter() {
    }
  }

  public static final class OptionalParameter<T> implements Param<T>{
    public OptionalParameter() {
    }
  }

  private final static Set<Class<?>> paramTypes = new HashSet<>(Arrays.asList(
      RequiredImpl.class, OptionalImpl.class, RequiredParameter.class,
      OptionalParameter.class));

  // Sets of things that have been declared but not used in a bind.
  // Both must be empty before build().
  private final Set<Field> reqUse = new HashSet<>();
  private final Set<Field> optUse = new HashSet<>();
  // Set of required unset parameters. Must be empty before build.
  private final Set<Field> reqSet = new HashSet<>();

  // Maps from field instance variables to the fields that they
  // are assigned to. These better be unique!
  private final Map<Object, Field> map = new HashMap<>();

  private final Map<Impl<?>, Class<?>> freeImpls = new HashMap<>();
  private final Map<Param<?>, Class<? extends Name<?>>> freeParams = new HashMap<>();
  private final Map<Impl<?>, Class<?>> setImpls = new HashMap<>();
  private final Map<Param<?>, String> setParams = new HashMap<>();
  
  public ConfigurationModule() {
    for (Field f : getClass().getDeclaredFields()) {
      Class<?> t = f.getType();
      if (paramTypes.contains(t)) {
        if (!Modifier.isPublic(f.getModifiers())) {
          throw new ClassHierarchyException(
              "Found a non-public configuration option in " + getClass() + ": "
                  + f);
        }
        if (!Modifier.isStatic(f.getModifiers())) {
          throw new ClassHierarchyException(
              "Found a non-static configuration option in " + getClass() + ": "
                  + f);
        }
        if (!Modifier.isFinal(f.getModifiers())) {
          throw new ClassHierarchyException(
              "Found a non-final configuration option in " + getClass() + ": "
                  + f);
        }
        final Object o;
        try {
          o = f.get(null);
        } catch (IllegalArgumentException | IllegalAccessException e) {
          throw new ClassHierarchyException(
              "Could not look up field instance in " + getClass() + " field: "
                  + f);
        }
        if (map.containsKey(o)) {
          throw new ClassHierarchyException(
              "Detected aliased instances in class " + getClass()
                  + " for fields " + map.get(o) + " and " + f);
        }
        if (t == RequiredImpl.class || t == RequiredParameter.class) {
          reqUse.add(f);
          reqSet.add(f);
        } else {
          optUse.add(f);
        }
        map.put(o, f);
      }
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public Configuration build() throws BindException {
    if(!(reqUse.isEmpty() && optUse.isEmpty())) {
      throw new ClassHierarchyException("Found declared options that were not used in binds: " + reqUse + " " + optUse);
    }
    if(!reqSet.isEmpty()) {
      throw new BindException("Attempt to build configuration before setting required option(s): " + reqSet);
    }
    
    for(Impl<?> i : setImpls.keySet()) {
      b.bind(freeImpls.get(i), setImpls.get(i));
    }
    for(Param<?> p : setParams.keySet()) {
      b.bindNamedParameter((Class)freeParams.get(p), setParams.get(p));
    }
    return b.build();
  }

  public <T> ConfigurationModule bind(Class<T> iface, Class<?> impl) {
    try {
      b.bind(iface, impl);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return this;
  }
  private <T> void processUse(Object impl) {
    Field f = map.get(impl);
    if(f == null) { /* throw */ }
    reqUse.remove(f);
    optUse.remove(f);
  }
  private <T> void processSet(Object impl) {
    Field f = map.get(impl);
    if(f == null) { /* throw */ }
    reqSet.remove(f);
  }
  public <T> ConfigurationModule set(Impl<T> opt, Class<? extends T> impl) {
    processSet(opt);
    setImpls.put(opt, impl);
    return this;
  }
  public <T> ConfigurationModule set(Param<T> opt, String val) {
    processSet(opt);
    setParams.put(opt, val);
    return this;
  }
  
  public <T> ConfigurationModule bind(Class<?> iface, Impl<?> opt) {
    processUse(opt);
    freeImpls.put(opt, iface);
    return this;
  }

  public <T> ConfigurationModule bindImplementation(Class<T> iface,
      Class<? extends T> impl) {
    try {
      b.bindImplementation(iface, impl);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public <T> ConfigurationModule bindImplementation(Class<T> iface, Impl<? extends T> opt) {
    processUse(opt);
    freeImpls.put(opt, iface);
    return this;
  }

  public <T> ConfigurationModule bindSingletonImplementation(Class<T> iface,
      Class<? extends T> impl) throws BindException {
    try {
      b.bindSingletonImplementation(iface, impl);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public <T> ConfigurationModule bindSingletonImplementation(Class<T> iface,
      Impl<? extends T> opt) throws BindException {
    processUse(opt);
    try {
      b.bindSingleton(iface);
      freeImpls.put(opt, iface);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public <T> ConfigurationModule bindSingleton(Class<T> iface) {
    try {
      b.bindSingleton(iface);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  public <T> ConfigurationModule bindNamedParameter(
      Class<? extends Name<T>> name, String value) {
    try {
      b.bindNamedParameter(name, value);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return this;
  }
  public <T> ConfigurationModule bindNamedParameter(
      Class<? extends Name<T>> name, Param<T> opt) {
    processUse(opt);
    freeParams.put(opt, name);
    return this;
  }

  public <T> ConfigurationModule bindNamedParameter(
      Class<? extends Name<T>> iface, Class<? extends T> impl) {
    try {
      b.bindNamedParameter(iface, impl);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return this;
  }
  public <T> ConfigurationModule bindNamedParameter(
      Class<? extends Name<T>> iface, Impl<? extends T> opt) {
    processUse(opt);
    freeImpls.put(opt, iface);
    return this;
  }

  public <T> ConfigurationModule bindConstructor(Class<T> c,
      Impl<? extends ExternalConstructor<? extends T>> v) {
    processUse(v);
    freeImpls.put(v, c);
    return this;
  }
}

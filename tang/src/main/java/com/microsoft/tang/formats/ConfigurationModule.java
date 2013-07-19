package com.microsoft.tang.formats;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.exceptions.NameResolutionException;
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
public abstract class ConfigurationModule {

  public interface Impl<T> {
  };

  public interface Param<T> {
  };

  public static final class RequiredImpl<T> implements Impl<T> {
  }

  public static final class OptionalImpl<T> implements Impl<T> {
  }

  public static final class RequiredParameter<T> implements Param<T> {
  }

  public static final class OptionalParameter<T> implements Param<T> {
  }

  private final static Set<Class<?>> paramTypes = new MonotonicHashSet<Class<?>>(
      RequiredImpl.class, OptionalImpl.class, RequiredParameter.class,
      OptionalParameter.class);

  final JavaConfigurationBuilder b = Tang.Factory.getTang()
      .newConfigurationBuilder();
  // Sets of things that have been declared
  private final Set<Field> reqDecl = new MonotonicHashSet<>();
  private final Set<Field> optDecl = new MonotonicHashSet<>();
  // Set of things that have been used in a bind. These must be equal
  // to the decl counterparts before build() is called.
  private final Set<Field> reqUsed = new MonotonicHashSet<>();
  private final Set<Field> optUsed = new MonotonicHashSet<>();
  // Set of required unset parameters. Must be empty before build.
  private final Set<Field> reqSet = new MonotonicHashSet<>();

  // Maps from field instance variables to the fields that they
  // are assigned to. These better be unique!
  private final Map<Object, Field> map = new MonotonicHashMap<>();

  private final Map<Class<?>, Impl<?>> freeImpls = new MonotonicHashMap<>();
  private final Map<Class<? extends Name<?>>,Param<?>> freeParams = new MonotonicHashMap<>();
  private final Map<Impl<?>, Class<?>> setImpls = new MonotonicHashMap<>();
  private final Map<Impl<?>, String> setLateImpls = new MonotonicHashMap<>();
  private final Map<Param<?>, String> setParams = new MonotonicHashMap<>();
  private final Set<Impl<?>> freeSingletons = new MonotonicHashSet<>();
  private final Map<Class<?>, String> lateBindClazz = new MonotonicHashMap<>();

  private final ConfigurationModule deepCopy() {
    // ooh... this is a dirty trick --- we strip this's type off here,
    // fortunately, we've all ready looked at the root object's class's
    // fields, and we copy the information we extracted from them, so
    // everything works out OK w.r.t. field detection.
    return new ConfigurationModule(this) {
    };
  }

  private ConfigurationModule(ConfigurationModule c) {
    try {
      b.addConfiguration(c.b.build());
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    reqDecl.addAll(c.reqDecl);
    optDecl.addAll(c.optDecl);
    reqUsed.addAll(c.reqUsed);
    optUsed.addAll(c.optUsed);
    reqSet.addAll(c.reqSet);
    map.putAll(c.map);
    freeImpls.putAll(c.freeImpls);
    freeParams.putAll(c.freeParams);
    setImpls.putAll(c.setImpls);
    setLateImpls.putAll(c.setLateImpls);
    setParams.putAll(c.setParams);
    freeSingletons.addAll(c.freeSingletons);
    lateBindClazz.putAll(c.lateBindClazz);
    
  }

  protected ConfigurationModule() {
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
          reqDecl.add(f);
        } else {
          optDecl.add(f);
        }
        map.put(o, f);
      }
    }
  }

  private final String toString(Set<Field> s) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Field f : s) {
      sb.append((first ? " " : ", ") + f.getName());
      first = false;
    }
    sb.append(" }");
    return sb.toString();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public final Configuration build() throws BindException {
    ConfigurationModule c = deepCopy();
    
    if (!(c.reqUsed.containsAll(c.reqDecl) && c.optUsed.containsAll(c.optDecl))) {
      Set<Field> fset = new MonotonicHashSet<>();
      for (Field f : c.reqDecl) {
        if (!c.reqUsed.contains(f)) {
          fset.add(f);
        }
      }
      for (Field f : c.optDecl) {
        if (!c.optUsed.contains(f)) {
          fset.add(f);
        }
      }
      throw new ClassHierarchyException(
          "Found declared options that were not used in binds: "
              + toString(fset));
    }
    if (!c.reqSet.containsAll(c.reqDecl)) {
      Set<Field> missingSet = new MonotonicHashSet<>();
      for (Field f : c.reqDecl) {
        if (!c.reqSet.contains(f)) {
          missingSet.add(f);
        }
      }
      throw new BindException(
          "Attempt to build configuration before setting required option(s): "
              + toString(missingSet));
    }

    for (Class<?> clz: c.lateBindClazz.keySet()) {
      try {
        c.b.bind(ReflectionUtilities.getFullName(clz), c.lateBindClazz.get(clz));
      } catch (NameResolutionException e) {
        throw new ClassHierarchyException("ConfigurationModule refers to unknown class: " + c.lateBindClazz.get(clz), e);
      }
    }
    
    for (Class<?> clazz : c.freeImpls.keySet()) {
      Impl<?> i = c.freeImpls.get(clazz);
      if(c.setImpls.containsKey(i)) {
        c.b.bind(clazz, c.setImpls.get(i));
      } else if(c.setLateImpls.containsKey(i)) {
        c.b.bind(ReflectionUtilities.getFullName(clazz), c.setLateImpls.get(i));
      }
    }
    for (Impl<?> i : c.freeSingletons) {
      if(c.setImpls.containsKey(i)) {
        c.b.bindSingleton(c.setImpls.get(i));
      } else if(c.setLateImpls.containsKey(i)) {
        c.b.bindSingleton(c.setLateImpls.get(i));
      }
    }
    for (Class<? extends Name<?>> clazz : c.freeParams.keySet()) {
      Param<?> p = c.freeParams.get(clazz);
      String s = c.setParams.get(p);
      if(s != null) {
        c.b.bindNamedParameter((Class)clazz, s);
      } else {
        if(!(p instanceof OptionalParameter)) {
          throw new IllegalStateException();
        }
      }
    }
    return c.b.build();
  }

  public final <T> ConfigurationModule bind(Class<T> iface, Class<?> impl) {
    ConfigurationModule c = deepCopy();
    try {
      c.b.bind(iface, impl);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  private final <T> void processUse(Object impl) {
    Field f = map.get(impl);
    if (f == null) { /* throw */
      if (f == null) { /* throw */
        throw new ClassHierarchyException("Unknown Impl/Param when binding " + ReflectionUtilities.getFullName(impl.getClass()) + " Did you pass in a field from some other module?");
      }
    }
    if(!reqUsed.contains(f)) { reqUsed.add(f); }
    if(!optUsed.contains(f)) { optUsed.add(f); }
  }

  private final <T> void processSet(Object impl) {
    Field f = map.get(impl);
    if (f == null) { /* throw */
      throw new ClassHierarchyException("Unknown Impl/Param when setting " + ReflectionUtilities.getFullName(impl.getClass()) + " Did you pass in a field from some other module?");
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

  public final <T> ConfigurationModule bind(Class<?> iface, Impl<?> opt) {
    ConfigurationModule c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(iface, opt);
    return c;
  }

  public final <T> ConfigurationModule bindImplementation(Class<T> iface,
      Class<? extends T> impl) {
    ConfigurationModule c = deepCopy();
    try {
      c.b.bindImplementation(iface, impl);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }
  public final <T> ConfigurationModule bindImplementation(Class<T> iface,
      String impl) {
    ConfigurationModule c = deepCopy();
    c.lateBindClazz.put(iface, impl);
    return c;
  }

  public final <T> ConfigurationModule bindImplementation(Class<T> iface,
      Impl<? extends T> opt) {
    ConfigurationModule c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(iface, opt);
    return c;
  }

  public final <T> ConfigurationModule bindSingletonImplementation(
      Class<T> iface, Class<? extends T> impl) throws BindException {
    ConfigurationModule c = deepCopy();
    try {
      c.b.bindSingletonImplementation(iface, impl);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModule bindSingletonImplementation(
      Class<T> iface, Impl<? extends T> opt) throws BindException {
    ConfigurationModule c = deepCopy();
    c.processUse(opt);
    try {
      c.b.bindSingleton(iface);
      c.freeImpls.put(iface, opt);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModule bindSingleton(Class<T> iface) {
    ConfigurationModule c = deepCopy();
    try {
      c.b.bindSingleton(iface);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }
  public final <T> ConfigurationModule bindSingleton(Impl<T> iface) {
    ConfigurationModule c = deepCopy();
    c.processUse(iface);
    c.freeSingletons.add(iface);
    return c;
  }

  public final <T> ConfigurationModule bindNamedParameter(
      Class<? extends Name<T>> name, String value) {
    ConfigurationModule c = deepCopy();
    try {
      c.b.bindNamedParameter(name, value);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModule bindNamedParameter(
      Class<? extends Name<T>> name, Param<T> opt) {
    ConfigurationModule c = deepCopy();
    c.processUse(opt);
    c.freeParams.put(name, opt);
    return c;
  }

  public final <T> ConfigurationModule bindNamedParameter(
      Class<? extends Name<T>> iface, Class<? extends T> impl) {
    ConfigurationModule c = deepCopy();
    try {
      c.b.bindNamedParameter(iface, impl);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModule bindNamedParameter(
      Class<? extends Name<T>> iface, Impl<? extends T> opt) {
    ConfigurationModule c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(iface, opt);
    return c;
  }

  public final <T> ConfigurationModule bindConstructor(Class<T> cons,
      Impl<? extends ExternalConstructor<? extends T>> v) {
    ConfigurationModule c = deepCopy();
    c.processUse(v);
    c.freeImpls.put(cons, v);
    return c;
  }
}

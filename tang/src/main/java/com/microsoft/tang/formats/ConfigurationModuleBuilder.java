package com.microsoft.tang.formats;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

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

public abstract class ConfigurationModuleBuilder {

  private final static Set<Class<?>> paramTypes = new MonotonicHashSet<Class<?>>(
      RequiredImpl.class, OptionalImpl.class, RequiredParameter.class,
      OptionalParameter.class);
  final JavaConfigurationBuilder b = Tang.Factory.getTang()
  .newConfigurationBuilder();
  // Sets of things that have been declared
  final Set<Field> reqDecl = new MonotonicHashSet<>();
  private final Set<Field> optDecl = new MonotonicHashSet<>();
  // Set of things that have been used in a bind. These must be equal
  // to the decl counterparts before build() is called.
  private final Set<Field> reqUsed = new MonotonicHashSet<>();
  private final Set<Field> optUsed = new MonotonicHashSet<>();
  // Maps from field instance variables to the fields that they
  // are assigned to. These better be unique!
  final Map<Object, Field> map = new MonotonicHashMap<>();
  final Map<Class<?>, Impl<?>> freeImpls = new MonotonicHashMap<>();
  final Map<Class<? extends Name<?>>,Param<?>> freeParams = new MonotonicHashMap<>();
  private final Map<Class<?>, String> lateBindClazz = new MonotonicHashMap<>();

  protected ConfigurationModuleBuilder() {
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

  private ConfigurationModuleBuilder(ConfigurationModuleBuilder c) {
    try {
      b.addConfiguration(c.b.build());
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    reqDecl.addAll(c.reqDecl);
    optDecl.addAll(c.optDecl);
    reqUsed.addAll(c.reqUsed);
    optUsed.addAll(c.optUsed);
    map.putAll(c.map);
    freeImpls.putAll(c.freeImpls);
    freeParams.putAll(c.freeParams);
    lateBindClazz.putAll(c.lateBindClazz);
    
  }
  public final <T> ConfigurationModuleBuilder bind(Class<?> iface, Impl<?> opt) {
    ConfigurationModuleBuilder c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(iface, opt);
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindImplementation(Class<T> iface,
      Class<? extends T> impl) {
    ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bindImplementation(iface, impl);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindImplementation(Class<T> iface,
      String impl) {
    ConfigurationModuleBuilder c = deepCopy();
    c.lateBindClazz.put(iface, impl);
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindImplementation(Class<T> iface,
      Impl<? extends T> opt) {
    ConfigurationModuleBuilder c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(iface, opt);
    return c;
  }

  @Deprecated
  public final <T> ConfigurationModuleBuilder bindSingletonImplementation(
      Class<T> iface, Class<? extends T> impl) throws BindException {
    ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bindSingletonImplementation(iface, impl);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindNamedParameter(
      Class<? extends Name<T>> name, String value) {
    ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bindNamedParameter(name, value);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindNamedParameter(
      Class<? extends Name<T>> name, Param<T> opt) {
    ConfigurationModuleBuilder c = deepCopy();
    c.processUse(opt);
    c.freeParams.put(name, opt);
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindNamedParameter(
      Class<? extends Name<T>> iface, Class<? extends T> impl) {
    ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bindNamedParameter(iface, impl);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindNamedParameter(
      Class<? extends Name<T>> iface, Impl<? extends T> opt) {
    ConfigurationModuleBuilder c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(iface, opt);
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindConstructor(Class<T> clazz,
      Class<? extends ExternalConstructor<? extends T>> constructor) {
    ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bindConstructor(clazz, constructor);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  public final <T> ConfigurationModuleBuilder bindConstructor(Class<T> cons,
      Impl<? extends ExternalConstructor<? extends T>> v) {
    ConfigurationModuleBuilder c = deepCopy();
    c.processUse(v);
    c.freeImpls.put(cons, v);
    return c;
  }

  private final <T> void processUse(Object impl) {
    Field f = map.get(impl);
    if (f == null) { /* throw */
      if (f == null) { /* throw */
        throw new ClassHierarchyException("Unknown Impl/Param when binding " + ReflectionUtilities.getSimpleName(impl.getClass()) + ".  Did you pass in a field from some other module?");
      }
    }
    if(!reqUsed.contains(f)) { reqUsed.add(f); }
    if(!optUsed.contains(f)) { optUsed.add(f); }
  }

  public final ConfigurationModule build() throws ClassHierarchyException {
    ConfigurationModuleBuilder c = deepCopy();
    
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
    for (Class<?> clz: c.lateBindClazz.keySet()) {
      try {
        c.b.bind(ReflectionUtilities.getFullName(clz), c.lateBindClazz.get(clz));
      } catch (NameResolutionException e) {
        throw new ClassHierarchyException("ConfigurationModule refers to unknown class: " + c.lateBindClazz.get(clz), e);
      } catch (BindException e) {
        throw new ClassHierarchyException("bind failed while initializing ConfigurationModuleBuilder", e);
      }
    }
    return new ConfigurationModule(c);
  }

  public final <T> ConfigurationModuleBuilder bind(Class<T> iface, Class<?> impl) {
    ConfigurationModuleBuilder c = deepCopy();
    try {
      c.b.bind(iface, impl);
    } catch (BindException e) {
      throw new ClassHierarchyException(e);
    }
    return c;
  }

  final ConfigurationModuleBuilder deepCopy() {
    // ooh... this is a dirty trick --- we strip this's type off here,
    // fortunately, we've all ready looked at the root object's class's
    // fields, and we copy the information we extracted from them, so
    // everything works out OK w.r.t. field detection.
    return new ConfigurationModuleBuilder(this) {
    };
  }
  final String toString(Set<Field> s) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Field f : s) {
      sb.append((first ? " " : ", ") + f.getName());
      first = false;
    }
    sb.append(" }");
    return sb.toString();
  }

}
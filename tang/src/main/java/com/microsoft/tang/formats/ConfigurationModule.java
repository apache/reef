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

public abstract class ConfigurationModule {

  public interface Impl<T> {
  };

  public interface Param<T> {
  };

  public static final class RequiredImpl<T> implements Impl<T> {
    public RequiredImpl() {
    }
  }

  public static final class OptionalImpl<T> implements Impl<T> {
    public OptionalImpl() {
    }
  }

  public static final class RequiredParameter<T> implements Param<T> {
    public RequiredParameter() {
    }
  }

  public static final class OptionalParameter<T> implements Param<T> {
    public OptionalParameter() {
    }
  }

  private final static Set<Class<?>> paramTypes = new HashSet<>(Arrays.asList(
      RequiredImpl.class, OptionalImpl.class, RequiredParameter.class,
      OptionalParameter.class));

  final JavaConfigurationBuilder b = Tang.Factory.getTang()
      .newConfigurationBuilder();
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

  private final ConfigurationModule deepCopy() {
    // ooh... this is a dirty trick --- we strip this's type off here,
    // fortunately, we've all ready looked at the root object's enclosing
    // class, so everything works out OK w.r.t. detecting fields in the
    // same module as us.
    return new ConfigurationModule(this) {
    };
  }

  private ConfigurationModule(ConfigurationModule c) {
    try {
      b.addConfiguration(c.b.build());
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    reqUse.addAll(c.reqUse);
    optUse.addAll(c.optUse);
    reqSet.addAll(c.reqSet);
    map.putAll(c.map);
    freeImpls.putAll(c.freeImpls);
    freeParams.putAll(c.freeParams);
    setImpls.putAll(c.setImpls);
    setParams.putAll(c.setParams);
  }

  public ConfigurationModule() {
    // TODO: Can we simply look at the stack to see what class invoked us?
    // That would avoid the crazy {}'s.
    if (getClass().getEnclosingClass() == null) {
      throw new ClassHierarchyException(
          "ConfigurationModules must be inner classes.  "
              + "In other words, put a '{}' between 'new ConfigurationModule()' "
              + "and the first '.bind...(...)'");
    }
    for (Field f : getClass().getEnclosingClass().getDeclaredFields()) {
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
    if (!(c.reqUse.isEmpty() && c.optUse.isEmpty())) {
      throw new ClassHierarchyException(
          "Found declared options that were not used in binds: "
              + toString(c.reqUse) + " " + toString(c.optUse));
    }
    if (!c.reqSet.isEmpty()) {
      throw new BindException(
          "Attempt to build configuration before setting required option(s): "
              + toString(c.reqSet));
    }

    for (Impl<?> i : c.setImpls.keySet()) {
      c.b.bind(c.freeImpls.get(i), c.setImpls.get(i));
    }
    for (Param<?> p : c.setParams.keySet()) {
      c.b.bindNamedParameter((Class) c.freeParams.get(p), c.setParams.get(p));
    }
    return c.b.build();
  }

  public final <T> ConfigurationModule bind(Class<T> iface, Class<?> impl) {
    ConfigurationModule c = deepCopy();
    try {
      c.b.bind(iface, impl);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return c;
  }

  private final <T> void processUse(Object impl) {
    Field f = map.get(impl);
    if (f == null) { /* throw */
    }
    reqUse.remove(f);
    optUse.remove(f);
  }

  private final <T> void processSet(Object impl) {
    Field f = map.get(impl);
    if (f == null) { /* throw */
    }
    reqSet.remove(f);
  }

  public final <T> ConfigurationModule set(Impl<T> opt, Class<? extends T> impl) {
    ConfigurationModule c = deepCopy();
    c.processSet(opt);
    c.setImpls.put(opt, impl);
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
    c.freeImpls.put(opt, iface);
    return c;
  }

  public final <T> ConfigurationModule bindImplementation(Class<T> iface,
      Class<? extends T> impl) {
    ConfigurationModule c = deepCopy();
    try {
      c.b.bindImplementation(iface, impl);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return c;
  }

  public final <T> ConfigurationModule bindImplementation(Class<T> iface,
      Impl<? extends T> opt) {
    ConfigurationModule c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(opt, iface);
    return c;
  }

  public final <T> ConfigurationModule bindSingletonImplementation(
      Class<T> iface, Class<? extends T> impl) throws BindException {
    ConfigurationModule c = deepCopy();
    try {
      c.b.bindSingletonImplementation(iface, impl);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return c;
  }

  public final <T> ConfigurationModule bindSingletonImplementation(
      Class<T> iface, Impl<? extends T> opt) throws BindException {
    ConfigurationModule c = deepCopy();
    c.processUse(opt);
    try {
      c.b.bindSingleton(iface);
      c.freeImpls.put(opt, iface);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return c;
  }

  public final <T> ConfigurationModule bindSingleton(Class<T> iface) {
    ConfigurationModule c = deepCopy();
    try {
      c.b.bindSingleton(iface);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return c;
  }

  public final <T> ConfigurationModule bindNamedParameter(
      Class<? extends Name<T>> name, String value) {
    ConfigurationModule c = deepCopy();
    try {
      c.b.bindNamedParameter(name, value);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return c;
  }

  public final <T> ConfigurationModule bindNamedParameter(
      Class<? extends Name<T>> name, Param<T> opt) {
    ConfigurationModule c = deepCopy();
    c.processUse(opt);
    c.freeParams.put(opt, name);
    return c;
  }

  public final <T> ConfigurationModule bindNamedParameter(
      Class<? extends Name<T>> iface, Class<? extends T> impl) {
    ConfigurationModule c = deepCopy();
    try {
      c.b.bindNamedParameter(iface, impl);
    } catch (BindException e) {
      throw new RuntimeException(e);
    }
    return c;
  }

  public final <T> ConfigurationModule bindNamedParameter(
      Class<? extends Name<T>> iface, Impl<? extends T> opt) {
    ConfigurationModule c = deepCopy();
    c.processUse(opt);
    c.freeImpls.put(opt, iface);
    return c;
  }

  public final <T> ConfigurationModule bindConstructor(Class<T> cons,
      Impl<? extends ExternalConstructor<? extends T>> v) {
    ConfigurationModule c = deepCopy();
    c.processUse(v);
    c.freeImpls.put(v, cons);
    return c;
  }
}

package com.microsoft.tang.implementation.java;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.microsoft.tang.ClassNode;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.ConstructorArg;
import com.microsoft.tang.ConstructorDef;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.NamedParameterNode;
import com.microsoft.tang.Node;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

public class ConfigurationBuilderImpl implements ConfigurationBuilder {
  // TODO: None of these should be public! - Move to configurationBuilder. Have
  // that wrap itself
  // in a sane Configuration interface...
  final ClassHierarchyImpl namespace;
  // TODO: getBindings(), getSingletons(), getLegacyConstructors().
  public final Map<ClassNode<?>, ClassNode<?>> boundImpls = new MonotonicMap<>();
  public final Map<ClassNode<?>, ClassNode<ExternalConstructor<?>>> boundConstructors = new MonotonicMap<>();
  public final Set<ClassNode<?>> singletons = new MonotonicSet<>();
  public final Map<NamedParameterNode<?>, String> namedParameters = new MonotonicMap<>();
  public final Map<ClassNode<?>, ConstructorDef<?>> legacyConstructors = new MonotonicMap<>();

  // *Not* serialized.
//  final Map<ClassNode<?>, Object> singletonInstances = new MonotonicMap<>();
  final Map<NamedParameterNode<?>, Object> namedParameterInstances = new MonotonicMap<>();

  public final static String IMPORT = "import";
  public final static String REGISTERED = "registered";
  public final static String SINGLETON = "singleton";
  public final static String INIT = "<init>";

  public ConfigurationBuilderImpl() {
    this.namespace = new ClassHierarchyImpl();
  }

  public ConfigurationBuilderImpl(URL... jars) {
    this.namespace = new ClassHierarchyImpl(jars);
  }

  public ConfigurationBuilderImpl(ClassLoader loader, URL... jars) {
    this.namespace = new ClassHierarchyImpl(loader, jars);
  }

  public ConfigurationBuilderImpl(ConfigurationBuilderImpl t) {
    this.namespace = new ClassHierarchyImpl();
    try {
      addConfiguration(t);
    } catch (BindException e) {
      throw new IllegalStateException("Could not copy builder", e);
    }
  }

  public ConfigurationBuilderImpl(Configuration... tangs) throws BindException {
    this.namespace = new ClassHierarchyImpl();
    for (Configuration tc : tangs) {
      addConfiguration(((ConfigurationImpl) tc));
    }
  }

  @Override
  public void addConfiguration(Configuration conf) throws BindException {
    // XXX remove cast!
    addConfiguration(((ConfigurationImpl) conf).builder);
  }

  private void addConfiguration(ConfigurationBuilderImpl builder)
      throws BindException {
    namespace.addJars(builder.namespace.getJars());

    for (String s : builder.namespace.getRegisteredClassNames()) {
      register(s);
    }
    // Note: The commented out lines would be faster, but, for testing
    // purposes,
    // we run through the high-level bind(), which dispatches to the correct
    // call.
    for (ClassNode<?> cn : builder.boundImpls.keySet()) {
      bind(cn.getFullName(), builder.boundImpls.get(cn).getFullName());
      // bindImplementation((Class<?>) cn.getClazz(), (Class)
      // t.boundImpls.get(cn));
    }
    for (ClassNode<?> cn : builder.boundConstructors.keySet()) {
      bind(cn.getFullName(), builder.boundConstructors.get(cn).getFullName());
      // bindConstructor((Class<?>) cn.getClazz(), (Class)
      // t.boundConstructors.get(cn));
    }
    for (ClassNode<?> cn : builder.singletons) {
      try {
        String fullName = cn.getFullName();
        bindSingleton(fullName);
/*        Object o = builder.singletonInstances.get(cn);
        if (o != null) {
          ClassNode<?> new_cn = (ClassNode<?>) namespace.register(fullName);
          singletons.add(new_cn);
          singletonInstances.put(new_cn, o);
        } else {
          bindSingleton(fullName);
        } */
      } catch (BindException e) {
        throw new IllegalStateException(
            "Unexpected BindException when copying ConfigurationBuilderImpl", e);
      }
    }
    // The namedParameters set contains the strings that can be used to
    // instantiate new
    // named parameter instances. Create new ones where we can.
    for (NamedParameterNode<?> np : builder.namedParameters.keySet()) {
      bind(np.getFullName(), builder.namedParameters.get(np));
    }
    // Copy references to the remaining (which must have been set with
    // bindVolatileParameter())
    for (NamedParameterNode<?> np : builder.namedParameterInstances.keySet()) {
      if (!builder.namedParameters.containsKey(np)) {
        Object o = builder.namedParameterInstances.get(np);
        NamedParameterNode<?> new_np = (NamedParameterNode<?>) namespace
            .register(np.getFullName());
        namedParameterInstances.put(new_np, o);
        if (o instanceof Class) {
          register((Class<?>) o);
        }
      }
    }
    for (ClassNode<?> cn : builder.legacyConstructors.keySet()) {
      registerLegacyConstructor(cn, builder.legacyConstructors.get(cn)
          .getArgs());
    }
  }

  /**
   * Needed when you want to make a class available for injection, but don't
   * want to bind a subclass to its implementation. Without this call, by the
   * time injector.newInstance() is called, ConfigurationBuilderImpl has been
   * locked down, and the class won't be found.
   * 
   * @param c
   */
  @Override
  public void register(Class<?> c) throws BindException {
    namespace.register(ReflectionUtilities.getFullName(c));
  }

  public void register(String s) throws BindException {
    namespace.register(s);
  }

  @Override
  public <T> void registerLegacyConstructor(Class<T> c, final Class<?>... args)
      throws BindException {
    String s = ReflectionUtilities.getFullName(c);
    registerLegacyConstructor(s, args);
  }

  @Override
  public void registerLegacyConstructor(ClassNode<?> c,
      final ConstructorArg... args) throws BindException {
    String cn[] = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      cn[i] = args[i].getType();
    }
    registerLegacyConstructor(c.getFullName(), cn);
  }

  @Override
  public void registerLegacyConstructor(String s, final String... args)
      throws BindException {
    ClassNode<?> cn = (ClassNode<?>) namespace.register(s);
    ClassNode<?>[] cnArgs = new ClassNode[args.length];
    for (int i = 0; i < args.length; i++) {
      cnArgs[i] = (ClassNode<?>) namespace.register(args[i]);
    }
    registerLegacyConstructor(cn, cnArgs);
  }

  @Override
  public void registerLegacyConstructor(String s, final Class<?>... args)
      throws BindException {
    ClassNode<?> cn = (ClassNode<?>) namespace.register(s);
    ClassNode<?>[] cnArgs = new ClassNode[args.length];
    for (int i = 0; i < args.length; i++) {
      cnArgs[i] = (ClassNode<?>) namespace.register(ReflectionUtilities
          .getFullName(args[i]));
    }
    registerLegacyConstructor(cn, cnArgs);
  }

  @Override
  public void registerLegacyConstructor(ClassNode<?> cn,
      final ClassNode<?>... args) throws BindException {
    legacyConstructors.put(cn, cn.getConstructorDef(args));
  }

  @Override
  public <T> void bind(String key, Class<?> value) throws BindException {
    bind(key, ReflectionUtilities.getFullName(value));
  }

  @Override
  public <T> void bind(String key, String value) throws BindException {
    Node n = namespace.register(key);
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, value);
    } else if (n instanceof ClassNode) {
      Class<?> v;
      Class<?> k;
      try {
        v = namespace.classForName(value);
      } catch (ClassNotFoundException e) {
        throw new BindException("Could not find class " + value);
      }
      try {
        k = namespace.classForName(key);
      } catch (ClassNotFoundException e) {
        throw new BindException("Could not find class " + key);
      }
      bind(k, v);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void bind(Class<T> c, Class<?> val) throws BindException {
    if (ExternalConstructor.class.isAssignableFrom(val)
        && (!ExternalConstructor.class.isAssignableFrom(c))) {
      bindConstructor(c,
          (Class<? extends ExternalConstructor<? extends T>>) val);
    } else {
      bindImplementation(c, (Class<? extends T>) val);
    }
  }

  @Override
  public <T> void bindImplementation(Class<T> c, Class<? extends T> d)
      throws BindException {
    if (!c.isAssignableFrom(d)) {
      throw new ClassCastException(d.getName()
          + " does not extend or implement " + c.getName());
    }

    Node n = namespace.register(ReflectionUtilities.getFullName(c));
    Node m = namespace.register(ReflectionUtilities.getFullName(d));

    if (n instanceof ClassNode) {
      if (m instanceof ClassNode) {
        boundImpls.put((ClassNode<?>) n, (ClassNode<?>) m);
      } else {
        throw new BindException("Cannot bind ClassNode " + n
            + " to non-ClassNode " + m);
      }
    } else {
      throw new BindException(
          "Detected type mismatch.  bindImplementation needs a ClassNode, but "
              + "namespace contains a " + n);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> void bindParameter(NamedParameterNode<T> name, String value)
      throws BindException {
    T o;
    try {
      o = ReflectionUtilities.parse(
          (Class<T>) namespace.classForName(name.getFullArgName()), value);
    } catch (ClassNotFoundException e) {
      throw new BindException("Can't parse unknown class "
          + name.getFullArgName());
    } catch (UnsupportedOperationException e) {
      try {
        o = (T) namespace.classForName(value);
      } catch (ClassNotFoundException e1) {
        throw new BindException("Do not know how to parse a "
            + name.getFullArgName()
            + " Furthermore, could not bind it to an implementation with name "
            + value);
      }
    }
    namedParameters.put(name, value);
    namedParameterInstances.put(name, o);
    if (o instanceof Class) {
      register((Class<?>) o);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void bindNamedParameter(Class<? extends Name<T>> name, String s)
      throws BindException {
    Node np = namespace.register(ReflectionUtilities.getFullName(name));
    if (np instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<T>) np, s);
    } else {
      throw new BindException(
          "Detected type mismatch when setting named parameter " + name
              + "  Expected NamedParameterNode, but namespace contains a " + np);
    }
  }

  @Override
  public <T> void bindNamedParameter(Class<? extends Name<T>> iface,
      Class<? extends T> impl) throws BindException {
    Node n = namespace.register(ReflectionUtilities.getFullName(iface));
    namespace.register(ReflectionUtilities.getFullName(impl));
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, impl.getName());
    } else {
      throw new BindException(
          "Detected type mismatch when setting named parameter " + iface
              + "  Expected NamedParameterNode, but namespace contains a " + n);
    }
  }

  @Override
  public <T> void bindSingleton(Class<T> c) throws BindException {
    bindSingleton(ReflectionUtilities.getFullName(c));
  }

  @Override
  public void bindSingleton(String s) throws BindException {
    Node n = namespace.register(s);
    if (!(n instanceof ClassNode)) {
      throw new IllegalArgumentException("Can't bind singleton to " + n
          + " try bindParameter() instead.");
    }
    ClassNode<?> cn = (ClassNode<?>) n;
    singletons.add(cn);
  }

  @Override
  public <T> void bindSingletonImplementation(Class<T> c, Class<? extends T> d)
      throws BindException {
    bindSingleton(c);
    bindImplementation(c, d);
  }

  @Override
  @SuppressWarnings({ "unchecked" })
  public <T> void bindConstructor(Class<T> c,
      Class<? extends ExternalConstructor<? extends T>> v) throws BindException {

    Node m = namespace.register(ReflectionUtilities.getFullName(v));
    try {
      boundConstructors
          .put((ClassNode<?>) namespace.register(ReflectionUtilities
              .getFullName(c)), (ClassNode<ExternalConstructor<?>>) m);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          "Cannot register external class constructor for " + c
              + " (which is probably a named parameter)");
    }
  }

  @Override
  public ConfigurationImpl build() {
    return new ConfigurationImpl(new ConfigurationBuilderImpl(this));
  }

  @Override
  public Collection<String> getShortNames() {
    return namespace.getShortNames();
  }

  @Override
  public String resolveShortName(String shortName) throws BindException {
    String ret = namespace.resolveShortName(shortName);
    if (ret == null) {
      throw new BindException("Could not find requested shortName:" + shortName);
    }
    return ret;
  }

  @Override
  public String classPrettyDefaultString(String longName) throws BindException {
    try {
      NamedParameterNode<?> param = (NamedParameterNode<?>) namespace
          .getNode(longName);
      return param.getSimpleArgName() + "="
          + param.getDefaultInstanceAsString();
    } catch (NameResolutionException e) {
      throw new BindException("Couldn't find " + longName
          + " when looking for default value", e);
    }
  }

  @Override
  public String classPrettyDescriptionString(String longName)
      throws BindException {
    try {
      NamedParameterNode<?> param = (NamedParameterNode<?>) namespace
          .getNode(longName);
      return param.getDocumentation() + "\n" + param.getFullName();
    } catch (NameResolutionException e) {
      throw new BindException("Couldn't find " + longName
          + " when looking for documentation string", e);
    }

  }
}

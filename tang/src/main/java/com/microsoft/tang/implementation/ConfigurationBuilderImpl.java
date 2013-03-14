package com.microsoft.tang.implementation;

import java.net.URL;
import java.util.Map;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.java.ClassHierarchyImpl;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

public class ConfigurationBuilderImpl implements ConfigurationBuilder {
  // TODO: None of these should be public! - Move to configurationBuilder. Have
  // that wrap itself
  // in a sane Configuration interface...
  // TODO: Should be final again!
  public ClassHierarchy namespace;
  final Map<ClassNode<?>, ClassNode<?>> boundImpls = new MonotonicMap<>();
  final Map<ClassNode<?>, ClassNode<? extends ExternalConstructor<?>>> boundConstructors = new MonotonicMap<>();
  final MonotonicSet<ClassNode<?>> singletons = new MonotonicSet<>();
  final Map<NamedParameterNode<?>, String> namedParameters = new MonotonicMap<>();
  final Map<ClassNode<?>, ConstructorDef<?>> legacyConstructors = new MonotonicMap<>();

  public final static String IMPORT = "import";
  public final static String REGISTERED = "registered";
  public final static String SINGLETON = "singleton";
  public final static String INIT = "<init>";

  public ConfigurationBuilderImpl() {
    this.namespace = Tang.Factory.getTang().getDefaultClassHierarchy();
  }

  public ConfigurationBuilderImpl(URL[] jars, Configuration[] confs)
      throws BindException {
    this.namespace = Tang.Factory.getTang().getDefaultClassHierarchy(jars);
    for (Configuration tc : confs) {
      addConfiguration(((ConfigurationImpl) tc));
    }
  }

  protected ConfigurationBuilderImpl(ConfigurationBuilderImpl t)
      throws BindException {
    this.namespace = t.getClassHierarchy();
    try {
      addConfiguration(t.getClassHierarchy(), t);
    } catch (BindException e) {
      throw new IllegalStateException("Could not copy builder", e);
    }
  }

  public ConfigurationBuilderImpl(URL... jars) throws BindException {
    this(jars, new Configuration[0]);
  }

  public ConfigurationBuilderImpl clone() {
    try {
      return new ConfigurationBuilderImpl(this);
    } catch (BindException e) {
      throw new IllegalStateException(
          "Caught BindException in clone().  Can't happen(?)", e);
    }
  }

  public ConfigurationBuilderImpl(Configuration... tangs) throws BindException {
    this(new URL[0], tangs);
  }

  @Override
  public void addConfiguration(Configuration conf) throws BindException {
    // XXX remove cast!
    addConfiguration(conf.getClassHierarchy(), ((ConfigurationImpl) conf).builder);
  }

  private void addConfiguration(ClassHierarchy ns, ConfigurationBuilderImpl builder)
      throws BindException {
    namespace = namespace.merge(ns);
    ((ClassHierarchyImpl) namespace).parameterParser
        .mergeIn(((ClassHierarchyImpl) namespace).parameterParser);

//    for (String s : ns.getRegisteredClassNames()) {
//      register(s);
//    }
    for (ClassNode<?> cn : builder.boundImpls.keySet()) {
      bind(cn.getFullName(), builder.boundImpls.get(cn).getFullName());
    }
    for (ClassNode<?> cn : builder.boundConstructors.keySet()) {
      bind(cn.getFullName(), builder.boundConstructors.get(cn).getFullName());
    }
    for (ClassNode<?> cn : builder.singletons) {
      try {
        bindSingleton(cn.getFullName());
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
    for (ClassNode<?> cn : builder.legacyConstructors.keySet()) {
      registerLegacyConstructor(cn, builder.legacyConstructors.get(cn)
          .getArgs());
    }
  }
  @Override
  public void register(String s) throws BindException {
    namespace.register(s);
  }
  @Override
  public ClassHierarchy getClassHierarchy() {
    return namespace;
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
  public void registerLegacyConstructor(ClassNode<?> cn,
      final ClassNode<?>... args) throws BindException {
    legacyConstructors.put(cn, cn.getConstructorDef(args));
  }

  @Override
  public <T> void bind(String key, String value) throws BindException {
    Node n = namespace.register(key);
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, value);
    } else if (n instanceof ClassNode) {
      Node m = namespace.register(value);
      bind((ClassNode<?>) n, (ClassNode<?>) m);
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void bind(Node key, Node value) throws BindException {
    if (key instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) key, value.getFullName());
    } else if (key instanceof ClassNode) {
      ClassNode<?> k = (ClassNode<?>) key;
      if (value instanceof ClassNode) {
        ClassNode<?> val = (ClassNode<?>) value;
        if (val.isExternalConstructor() && !k.isExternalConstructor()) {
          bindConstructor(k, (ClassNode) val);
        } else {
          bindImplementation(k, (ClassNode) val);
        }
      }
    }
  }

  public <T> void bindImplementation(ClassNode<T> n, ClassNode<? extends T> m)
      throws BindException {
    if (namespace.isImplementation(n, m)) {
      boundImpls.put(n, m);
    } else {
      throw new IllegalArgumentException("Class" + m + " does not extend " + n);
    }
  }

  public <T> void bindParameter(NamedParameterNode<T> name, String value)
      throws BindException {
    T o = namespace.parse(name, value);
    if (o instanceof Class) {
      register(ReflectionUtilities.getFullName((Class<?>) o));
    }
    namedParameters.put(name, value);
  }

  @Override
  public void bindSingleton(ClassNode<?> n) throws BindException {
    singletons.add((ClassNode<?>) n);
  }

  @Override
  public void bindSingleton(String s) throws BindException {
    Node n = namespace.register(s);
    if (!(n instanceof ClassNode)) {
      throw new IllegalArgumentException("Can't bind singleton to " + n
          + " try bindNamedParameter() instead (named parameters are always singletons)");
    }
    bindSingleton((ClassNode<?>) n);
  }

  @Override
  public <T> void bindSingletonImplementation(ClassNode<T> c,
      ClassNode<? extends T> d) throws BindException {
    bindImplementation(c, d);
    bindSingleton(c);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void bindSingletonImplementation(String inter, String impl)
      throws BindException {
    Node cn = namespace.register(inter);
    Node dn = namespace.register(impl);
    if (!(cn instanceof ClassNode)) {
      throw new BindException(
          "bindSingletonImplementation was passed interface " + inter
              + ", which did not resolve to a ClassNode");
    }
    if (!(dn instanceof ClassNode)) {
      throw new BindException(
          "bindSingletonImplementation was passed implementation " + impl
              + ", which did not resolve to a ClassNode");
    }
    bindImplementation((ClassNode) cn, (ClassNode) dn);
    bindSingleton((ClassNode<?>) cn);
  }

  @Override
  public <T> void bindConstructor(ClassNode<T> k,
      ClassNode<? extends ExternalConstructor<? extends T>> v)
      throws BindException {
    boundConstructors.put(k, v);
  }

  @Override
  public ConfigurationImpl build() {
    return new ConfigurationImpl(this.clone());
  }

  @Override
  public String classPrettyDefaultString(String longName) throws BindException {
    NamedParameterNode<?> param = (NamedParameterNode<?>) namespace
        .register(longName);
    if (param == null) {
      throw new BindException("Couldn't find " + longName
          + " when looking for default value");
    }
    return param.getSimpleArgName() + "=" + param.getDefaultInstanceAsString();
  }

  @Override
  public String classPrettyDescriptionString(String longName)
      throws BindException {
    NamedParameterNode<?> param = (NamedParameterNode<?>) namespace
        .register(longName);
    if (param == null) {
      throw new BindException("Couldn't find " + longName
          + " when looking for documentation string");
    }
    return param.getDocumentation() + "\n" + param.getFullName();
  }
}

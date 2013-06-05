package com.microsoft.tang.implementation;

import java.net.URL;
import java.util.Map;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.JavaClassHierarchy;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.java.ClassHierarchyImpl;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.TracingMonotonicMap;

public class ConfigurationBuilderImpl implements ConfigurationBuilder {
  // TODO: None of these should be public! - Move to configurationBuilder. Have
  // that wrap itself
  // in a sane Configuration interface...
  // TODO: Should be final again!
  public ClassHierarchy namespace;
  final Map<ClassNode<?>, ClassNode<?>> boundImpls = new TracingMonotonicMap<>();
  final Map<ClassNode<?>, ClassNode<? extends ExternalConstructor<?>>> boundConstructors = new TracingMonotonicMap<>();
  final MonotonicSet<ClassNode<?>> singletons = new MonotonicSet<>();
  final Map<NamedParameterNode<?>, String> namedParameters = new TracingMonotonicMap<>();
  final Map<ClassNode<?>, ConstructorDef<?>> legacyConstructors = new TracingMonotonicMap<>();

  public final static String IMPORT = "import";
  public final static String SINGLETON = "singleton";
  public final static String INIT = "<init>";

  protected ConfigurationBuilderImpl() {
    this.namespace = Tang.Factory.getTang().getDefaultClassHierarchy();
  }

  protected ConfigurationBuilderImpl(URL[] jars, Configuration[] confs, Class<? extends ExternalConstructor<?>>[] parsers)
      throws BindException {
    this.namespace = Tang.Factory.getTang().getDefaultClassHierarchy(jars,parsers);
    for (Configuration tc : confs) {
      addConfiguration(((ConfigurationImpl) tc));
    }
  }

  protected ConfigurationBuilderImpl(ConfigurationBuilderImpl t) {
    this.namespace = t.getClassHierarchy();
    try {
      addConfiguration(t.getClassHierarchy(), t);
    } catch (BindException e) {
      throw new IllegalStateException("Could not copy builder", e);
    }
  }

  @SuppressWarnings("unchecked")
  protected ConfigurationBuilderImpl(URL... jars) throws BindException {
    this(jars, new Configuration[0], new Class[0]);
  }

  @SuppressWarnings("unchecked")
  protected ConfigurationBuilderImpl(Configuration... confs) throws BindException {
    this(new URL[0], confs, new Class[0]);
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
    ClassNode<?> cn = (ClassNode<?>) namespace.getNode(s);
    ClassNode<?>[] cnArgs = new ClassNode[args.length];
    for (int i = 0; i < args.length; i++) {
      cnArgs[i] = (ClassNode<?>) namespace.getNode(args[i]);
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
    Node n = namespace.getNode(key);
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, value);
    } else if (n instanceof ClassNode) {
      Node m = namespace.getNode(value);
      bind((ClassNode<?>) n, (ClassNode<?>) m);
    } else {
      throw new IllegalStateException("getNode() returned " + n + " which is neither a ClassNode nor a NamedParameterNode");
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
    /* Parse and discard value; this is just for type checking */
    if(namespace instanceof JavaClassHierarchy) {
      ((JavaClassHierarchy)namespace).parse(name, value);
    }
    namedParameters.put(name, value);
  }

  @Override
  public void bindSingleton(ClassNode<?> n) throws BindException {
    singletons.add((ClassNode<?>) n);
  }

  @Override
  public void bindSingleton(String s) throws BindException {
    Node n = namespace.getNode(s);
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
    Node cn = namespace.getNode(inter);
    Node dn = namespace.getNode(impl);
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
      ClassNode<? extends ExternalConstructor<? extends T>> v) {
    boundConstructors.put(k, v);
  }

  @Override
  public ConfigurationImpl build() {
    return new ConfigurationImpl(new ConfigurationBuilderImpl(this));
  }

  @Override
  public String classPrettyDefaultString(String longName) throws NameResolutionException {
    final NamedParameterNode<?> param = (NamedParameterNode<?>) namespace
        .getNode(longName);
    return param.getSimpleArgName() + "=" + param.getDefaultInstanceAsString();
  }

  @Override
  public String classPrettyDescriptionString(String fullName)
      throws NameResolutionException {
    final NamedParameterNode<?> param = (NamedParameterNode<?>) namespace
      .getNode(fullName);
    return param.getDocumentation() + "\n" + param.getFullName();
  }
}

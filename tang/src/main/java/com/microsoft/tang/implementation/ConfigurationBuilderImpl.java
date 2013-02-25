package com.microsoft.tang.implementation;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.ClassNode;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.ConstructorArg;
import com.microsoft.tang.ConstructorDef;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.NamedParameterNode;
import com.microsoft.tang.Node;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ParameterParser;
import com.microsoft.tang.implementation.java.ClassHierarchyImpl;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

public class ConfigurationBuilderImpl implements ConfigurationBuilder {
  // TODO: None of these should be public! - Move to configurationBuilder. Have
  // that wrap itself
  // in a sane Configuration interface...
  public final ClassHierarchy namespace;
  // TODO: getBindings(), getSingletons(), getLegacyConstructors().
  public final Map<ClassNode<?>, ClassNode<?>> boundImpls = new MonotonicMap<>();
  public final Map<ClassNode<?>, ClassNode<? extends ExternalConstructor<?>>> boundConstructors = new MonotonicMap<>();
  public final Set<ClassNode<?>> singletons = new MonotonicSet<>();
  public final Map<NamedParameterNode<?>, String> namedParameters = new MonotonicMap<>();
  public final Map<ClassNode<?>, ConstructorDef<?>> legacyConstructors = new MonotonicMap<>();

  public final static String IMPORT = "import";
  public final static String REGISTERED = "registered";
  public final static String SINGLETON = "singleton";
  public final static String INIT = "<init>";

  public final ParameterParser parameterParser = new ParameterParser();

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
    parameterParser.mergeIn(builder.parameterParser);

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

  public void register(String s) throws BindException {
    namespace.register(s);
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

  // TODO: Fix up the exception handling surrounding parsing of values
  public <T> T parseDefaultValue(NamedParameterNode<T> name) throws InjectionException {
    try {
      String val = name.getDefaultInstanceAsString();
      if (val != null) {
        return parse(name, val);
      } else {
        return null;
      }
    } catch (BindException e) {
      throw new InjectionException("Could not parse " + name + "=" + "value", e);
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T parse(NamedParameterNode<T> name, String value) throws BindException {
    return (T) parse(name.getFullArgName(), value);
  }

  private Object parse(String name, String value) throws BindException {
    try {
      try {
        return parameterParser.parse(name, value);
      } catch (UnsupportedOperationException e) {
        return ((ClassHierarchyImpl) namespace).classForName(value);
      }
    } catch (ClassNotFoundException e) {
      throw new BindException("Could not parse type " + name + ".  Value was "
          + value, e);
    }
  }

  public <T> void bindParameter(NamedParameterNode<T> name, String value)
      throws BindException {
    T o = parse(name, value);
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
          + " try bindParameter() instead.");
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

package com.microsoft.tang.implementation.java;

import java.net.URL;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.ConfigurationBuilderImpl;
import com.microsoft.tang.implementation.ConfigurationImpl;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ExternalConstructor;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.ReflectionUtilities;

public class JavaConfigurationBuilderImpl extends ConfigurationBuilderImpl
    implements JavaConfigurationBuilder {

  public JavaConfigurationBuilderImpl(URL[] jars) {
    super(jars);
  }

  protected JavaConfigurationBuilderImpl(JavaConfigurationBuilderImpl impl) {
    super(impl);
  }

  public JavaConfigurationBuilderImpl(Configuration[] confs)
      throws BindException {
    super(confs);
  }

  @Override
  public JavaConfigurationBuilderImpl clone() {
    return new JavaConfigurationBuilderImpl(this);
  }
  
  @Override
  public ConfigurationImpl build() {
    return new ConfigurationImpl(new JavaConfigurationBuilderImpl(this));
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
  public Node register(Class<?> c) throws BindException {
    return namespace.register(ReflectionUtilities.getFullName(c));
  }

  @Override
  public <T> void bind(Class<T> c, Class<?> val) throws BindException {
    bind(register(c), register(val));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void bindImplementation(Class<T> c, Class<? extends T> d)
      throws BindException {
    Node cn = register(c);
    Node dn = register(d);
    if (!(cn instanceof ClassNode)) {
      throw new BindException(
          "bindImplementation passed interface that resolved to " + cn
              + " expected a ClassNode<?>");
    }
    if (!(dn instanceof ClassNode)) {
      throw new BindException(
          "bindImplementation passed implementation that resolved to " + dn
              + " expected a ClassNode<?>");
    }
    bindImplementation((ClassNode<T>) register(c),
        (ClassNode<? extends T>) register(d));
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
    Node n = register(iface);
    if (!(n instanceof NamedParameterNode)) {
      throw new BindException("Type mismatch when setting named parameter " + n
          + " Expected NamedParameterNode");
    }
    bind(register(iface), register(impl));
  }

  @Override
  public <T> void bindSingleton(Class<T> c) throws BindException {
    bindSingleton(ReflectionUtilities.getFullName(c));
  }

  @Override
  public <T> void bindSingletonImplementation(Class<T> c, Class<? extends T> d)
      throws BindException {
    bindSingleton(c);
    bindImplementation(c, d);
  }

  @Override
  public void bindParser(Class<? extends ExternalConstructor<?>> ec)
      throws BindException {
    parameterParser.addParser(ec);
  }

  @SuppressWarnings({ "unchecked" })
  public <T> void bindConstructor(Class<T> c,
      Class<? extends ExternalConstructor<? extends T>> v) throws BindException {
    Node n = namespace.register(ReflectionUtilities.getFullName(c));
    Node m = namespace.register(ReflectionUtilities.getFullName(v));
    if(!(n instanceof ClassNode)) {
      throw new BindException("BindConstructor got class that resolved to " + n + "; expected ClassNode");
    }
    if(!(m instanceof ClassNode)) {
      throw new BindException("BindConstructor got class that resolved to " + m + "; expected ClassNode");
    }
    bindConstructor((ClassNode<T>)n, (ClassNode<? extends ExternalConstructor<? extends T>>)m);
  }

}

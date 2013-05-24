package com.microsoft.tang.implementation.java;

import java.net.URL;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.JavaClassHierarchy;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.ConfigurationBuilderImpl;
import com.microsoft.tang.implementation.ConfigurationImpl;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.ReflectionUtilities;

public class JavaConfigurationBuilderImpl extends ConfigurationBuilderImpl
    implements JavaConfigurationBuilder {

  JavaConfigurationBuilderImpl(URL[] jars, Configuration[] confs)
      throws BindException {
    super(jars,confs);
  }
  JavaConfigurationBuilderImpl(){
    super();
  }
  public JavaConfigurationBuilderImpl(URL[] jars) throws BindException {
    super(jars);
  }

  JavaConfigurationBuilderImpl(JavaConfigurationBuilderImpl impl) {
    super(impl);
  }

  public JavaConfigurationBuilderImpl(Configuration[] confs)
      throws BindException {
    super(confs);
  }
  private class JavaConfigurationImpl extends ConfigurationImpl {
    JavaConfigurationImpl(JavaConfigurationBuilderImpl builder) {
      super(builder);
    }
  }
  @Override
  public ConfigurationImpl build() {
    return new JavaConfigurationImpl(new JavaConfigurationBuilderImpl(this));
  }

  private Node getNode(Class<?> c) {
    return ((JavaClassHierarchy)namespace).getNode(c);
  }
  
  @Override
  public <T> void bind(Class<T> c, Class<?> val) throws BindException {
    bind(getNode(c), getNode(val));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void bindImplementation(Class<T> c, Class<? extends T> d)
      throws BindException {
    Node cn = getNode(c);
    Node dn = getNode(d);
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
    bindImplementation((ClassNode<T>) cn, (ClassNode<? extends T>) dn);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void bindNamedParameter(Class<? extends Name<T>> name, String s)
      throws BindException {
    Node np = getNode(name);
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
    Node ifaceN = getNode(iface);
    Node implN = getNode(impl);
    if (!(ifaceN instanceof NamedParameterNode)) {
      throw new BindException("Type mismatch when setting named parameter " + ifaceN
          + " Expected NamedParameterNode");
    }
    bind(ifaceN, implN);
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
    ((ClassHierarchyImpl)namespace).parameterParser.addParser(ec);
  }

  @Override
  public <T, U extends T> void bindParser(Class<U> c, Class<? extends ExternalConstructor<T>> ec) 
      throws BindException {
    ((ClassHierarchyImpl)namespace).parameterParser.addParser(c, ec);
  }

  @SuppressWarnings({ "unchecked" })
  public <T> void bindConstructor(Class<T> c,
      Class<? extends ExternalConstructor<? extends T>> v) throws BindException {
    Node n = getNode(c);
    Node m = getNode(v);
    if(!(n instanceof ClassNode)) {
      throw new BindException("BindConstructor got class that resolved to " + n + "; expected ClassNode");
    }
    if(!(m instanceof ClassNode)) {
      throw new BindException("BindConstructor got class that resolved to " + m + "; expected ClassNode");
    }
    bindConstructor((ClassNode<T>)n, (ClassNode<? extends ExternalConstructor<? extends T>>)m);
  }
}

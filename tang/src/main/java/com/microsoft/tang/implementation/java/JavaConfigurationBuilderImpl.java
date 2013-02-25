package com.microsoft.tang.implementation.java;

import java.net.URL;

import com.microsoft.tang.ClassNode;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.NamedParameterNode;
import com.microsoft.tang.Node;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.util.ReflectionUtilities;

public class JavaConfigurationBuilderImpl extends ConfigurationBuilderImpl implements JavaConfigurationBuilder {

  public JavaConfigurationBuilderImpl(URL[] jars) {
    super(jars);
  }
  public JavaConfigurationBuilderImpl(JavaConfigurationBuilderImpl impl) {
    super(impl);
  }
  public JavaConfigurationBuilderImpl(Configuration[] confs) throws BindException {
    super(confs);
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
  @SuppressWarnings("unchecked")
  @Override
  public <T> ClassNode<T> register(Class<T> c) throws BindException {
    return (ClassNode<T>) namespace.register(ReflectionUtilities.getFullName(c));
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
  @SuppressWarnings("unchecked")
  public <T> void bindImplementation(Class<T> c, Class<? extends T> d)
      throws BindException {
    if (!c.isAssignableFrom(d)) {
      throw new ClassCastException(d.getName()
          + " does not extend or implement " + c.getName());
    }
    Node n = namespace.register(ReflectionUtilities.getFullName(c));
    Node m =  namespace.register(ReflectionUtilities.getFullName(d));
    if (n instanceof ClassNode) {
      if (m instanceof ClassNode) {
        bindImplementation((ClassNode<T>)n,(ClassNode<? extends T>)m);
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
  public <T> void bindSingletonImplementation(Class<T> c, Class<? extends T> d)
      throws BindException {
    bindSingleton(c);
    bindImplementation(c, d);
  }
  @Override
  public void bindParser(Class<? extends ExternalConstructor<?>> ec) throws BindException {
    parameterParser.addParser(ec);
  }
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


}

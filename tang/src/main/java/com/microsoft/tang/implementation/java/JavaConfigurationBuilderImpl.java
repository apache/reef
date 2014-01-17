/**
 * Copyright (C) 2012 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.tang.implementation.java;

import java.lang.reflect.Type;
import java.net.URL;
import java.util.Set;

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

  public JavaConfigurationBuilderImpl(URL[] jars, Configuration[] confs, Class<? extends ExternalConstructor<?>>[] parsers)
            throws BindException {
        super(jars,confs,parsers);
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
  public void bindNamedParameter(Class<? extends Name<?>> name, String s)
      throws BindException {
    Node np = getNode(name);
    if (np instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) np, s);
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
  }

  @Override
  public <T> void bindSingletonImplementation(Class<T> c, Class<? extends T> d)
      throws BindException {
    bindImplementation(c,d);
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
  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(Class<? extends Name<Set<T>>> iface, String value) throws BindException {
    Node n = getNode(iface);
    
    if(!(n instanceof NamedParameterNode)) {
      throw new BindException("BindSetEntry got an interface that resolved to " + n + "; expected a NamedParameter");
    }
    Type setType = ReflectionUtilities.getInterfaceTarget(Name.class, iface);
    if(!ReflectionUtilities.getRawClass(setType).equals(Set.class)) {
      throw new BindException("BindSetEntry got a NamedParameter that takes a " + setType + "; expected Set<...>");
    }
//    Type valType = ReflectionUtilities.getInterfaceTarget(Set.class, setType);
    bindSetEntry((NamedParameterNode<Set<T>>)n, value);
  }
  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSetEntry(Class<? extends Name<Set<T>>> iface, Class<? extends T> impl) throws BindException {
    Node n = getNode(iface);
    Node m = getNode(impl);
    
    if(!(n instanceof NamedParameterNode)) {
      throw new BindException("BindSetEntry got an interface that resolved to " + n + "; expected a NamedParameter");
    }
    Type setType = ReflectionUtilities.getInterfaceTarget(Name.class, iface);
    if(!ReflectionUtilities.getRawClass(setType).equals(Set.class)) {
      throw new BindException("BindSetEntry got a NamedParameter that takes a " + setType + "; expected Set<...>");
    }
    Type valType = ReflectionUtilities.getInterfaceTarget(Set.class, setType);
    if(!ReflectionUtilities.getRawClass(valType).isAssignableFrom(impl)) {
      throw new BindException("BindSetEntry got implementation " + impl + " that is incompatible with expected type " + valType);
    }
    
    bindSetEntry((NamedParameterNode<Set<T>>)n,m);
    
  }
}

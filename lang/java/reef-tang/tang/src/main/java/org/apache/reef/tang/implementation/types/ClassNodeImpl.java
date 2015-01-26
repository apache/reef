/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.tang.implementation.types;

import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.types.ClassNode;
import org.apache.reef.tang.types.ConstructorDef;
import org.apache.reef.tang.types.Node;
import org.apache.reef.tang.util.MonotonicSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ClassNodeImpl<T> extends AbstractNode implements ClassNode<T> {
  private final boolean injectable;
  private final boolean unit;
  private final boolean externalConstructor;
  private final ConstructorDef<T>[] injectableConstructors;
  private final ConstructorDef<T>[] allConstructors;
  private final MonotonicSet<ClassNode<T>> knownImpls;
  private final String defaultImpl;

  public ClassNodeImpl(Node parent, String simpleName, String fullName,
                       boolean unit, boolean injectable, boolean externalConstructor,
                       ConstructorDef<T>[] injectableConstructors,
                       ConstructorDef<T>[] allConstructors,
                       String defaultImplementation) {
    super(parent, simpleName, fullName);
    this.unit = unit;
    this.injectable = injectable;
    this.externalConstructor = externalConstructor;
    this.injectableConstructors = injectableConstructors;
    this.allConstructors = allConstructors;
    this.knownImpls = new MonotonicSet<>();
    this.defaultImpl = defaultImplementation;
  }

  @Override
  public ConstructorDef<T>[] getInjectableConstructors() {
    return injectableConstructors;
  }

  @Override
  public ConstructorDef<T>[] getAllConstructors() {
    return allConstructors;
  }

  @Override
  public boolean isInjectionCandidate() {
    return injectable;
  }

  @Override
  public boolean isExternalConstructor() {
    return externalConstructor;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString() + ": ");
    if (getInjectableConstructors() != null) {
      for (ConstructorDef<T> c : getInjectableConstructors()) {
        sb.append(c.toString() + ", ");
      }
    } else {
      sb.append("OBJECT BUILD IN PROGRESS!  BAD NEWS!");
    }
    return sb.toString();
  }

  public ConstructorDef<T> getConstructorDef(ClassNode<?>... paramTypes)
      throws BindException {
    if (!isInjectionCandidate()) {
      throw new BindException("Cannot @Inject non-static member/local class: "
          + getFullName());
    }
    for (ConstructorDef<T> c : getAllConstructors()) {
      if (c.takesParameters(paramTypes)) {
        return c;
      }
    }
    throw new BindException("Could not find requested constructor for class "
        + getFullName());
  }

  @Override
  public void putImpl(ClassNode<T> impl) {
    knownImpls.add(impl);
  }

  @Override
  public Set<ClassNode<T>> getKnownImplementations() {
    return new MonotonicSet<>(knownImpls);
  }

  @Override
  public boolean isUnit() {
    return unit;
  }

  @Override
  public boolean isImplementationOf(ClassNode<?> inter) {
    List<ClassNode<?>> worklist = new ArrayList<>();
    if (this.equals(inter)) {
      return true;
    }
    worklist.add(inter);
    while (!worklist.isEmpty()) {
      ClassNode<?> cn = worklist.remove(worklist.size() - 1);
      @SuppressWarnings({"rawtypes", "unchecked"})
      Set<ClassNode<?>> impls = (Set) cn.getKnownImplementations();
      if (impls.contains(this)) {
        return true;
      }
      worklist.addAll(impls);
    }
    return false;
  }

  @Override
  public String getDefaultImplementation() {
    return defaultImpl;
  }
}
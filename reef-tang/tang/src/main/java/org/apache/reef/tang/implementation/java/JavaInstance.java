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
package org.apache.reef.tang.implementation.java;

import org.apache.reef.tang.implementation.InjectionPlan;
import org.apache.reef.tang.types.Node;

final public class JavaInstance<T> extends InjectionPlan<T> {
  final T instance;

  public JavaInstance(Node name, T instance) {
    super(name);
    this.instance = instance;
  }

  @Override
  public int getNumAlternatives() {
    return instance == null ? 0 : 1;
  }

  @Override
  public String toString() {
    return getNode() + " = " + instance;
  }

  @Override
  public boolean isAmbiguous() {
    return false;
  }

  @Override
  public boolean isInjectable() {
    return instance != null;
  }

  public String getInstanceAsString() {
    return instance.toString();
  }

  @Override
  protected String toAmbiguousInjectString() {
    throw new IllegalArgumentException("toAmbiguousInjectString called on JavaInstance!" + this.toString());
  }

  @Override
  protected String toInfeasibleInjectString() {
    return getNode() + " is not bound.";
  }

  @Override
  protected boolean isInfeasibleLeaf() {
    return true;
  }

  @Override
  public String toShallowString() {
    return toString();
  }
}
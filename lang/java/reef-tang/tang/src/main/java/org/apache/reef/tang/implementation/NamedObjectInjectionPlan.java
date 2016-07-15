/*
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

package org.apache.reef.tang.implementation;

import org.apache.reef.tang.types.NamedObjectElement;

public class NamedObjectInjectionPlan<T> extends InjectionPlan<T> {
  private final InjectionPlan<? extends T> typePlan;
  private final NamedObjectElement namedObjectElement;

  public NamedObjectInjectionPlan(final NamedObjectElement namedObjectElement,
                                  final InjectionPlan<? extends T> typePlan) {
    super(namedObjectElement.getTypeNode());
    this.namedObjectElement = namedObjectElement;
    this.typePlan = typePlan;
  }

  public NamedObjectElement getNamedObjectElement() {
    return namedObjectElement;
  }

  public InjectionPlan<? extends T> getTypePlan() {
    return typePlan;
  }
  @Override
  public int getNumAlternatives() {
    return typePlan.getNumAlternatives();
  }

  @Override
  public boolean isAmbiguous() {
    return typePlan.isAmbiguous();
  }

  @Override
  public boolean isInjectable() {
    return typePlan.isInjectable();
  }

  @Override
  protected String toAmbiguousInjectString() {
    String s = "Plan of [" + namedObjectElement.getTypeNode().getFullName() +
        ", " + namedObjectElement.getName() + "] (NamedObject) is Ambiguous: \n";
    s += typePlan.toInfeasibleInjectString();
    return s;
  }

  @Override
  protected String toInfeasibleInjectString() {
    String s = "Plan of [" + namedObjectElement.getTypeNode().getFullName() +
        ", " + namedObjectElement.getName() + "] (NamedObject) is Infeasible: \n";
    s += typePlan.toInfeasibleInjectString();
    return s;
  }

  @Override
  protected boolean isInfeasibleLeaf() {
    return false;
  }

  @Override
  public String toShallowString() {
    return "NamedObjectInjectionPlan<" + getNode().getFullName() + ", " + getNamedObjectElement().getName() + ">";
  }

}

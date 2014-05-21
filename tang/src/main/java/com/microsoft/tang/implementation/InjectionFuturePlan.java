/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.tang.implementation;

import com.microsoft.tang.types.Node;

public class InjectionFuturePlan<T> extends InjectionPlan<T> {

  public InjectionFuturePlan(Node name) {
    super(name);
  }

  @Override
  public int getNumAlternatives() {
    return 1;
  }

  @Override
  public boolean isAmbiguous() {
    return false;
  }

  @Override
  public boolean isInjectable() {
    return true;
  }

  @Override
  protected String toAmbiguousInjectString() {
    throw new UnsupportedOperationException("InjectionFuturePlan cannot be ambiguous!");
  }

  @Override
  protected String toInfeasibleInjectString() {
    throw new UnsupportedOperationException("InjectionFuturePlan is always feasible!");
  }

  @Override
  protected boolean isInfeasibleLeaf() {
    return false;
  }

  @Override
  public String toShallowString() {
    return "InjectionFuture<" + getNode().getFullName() + ">";
  }

}

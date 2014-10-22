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
package com.microsoft.tang.util.walk;

import com.microsoft.tang.implementation.InjectionPlan;
import com.microsoft.tang.implementation.Constructor;
import com.microsoft.tang.implementation.Subplan;
import com.microsoft.tang.implementation.java.JavaInstance;

/**
 * Generic interface to traverse nodes of the injection plan.
 * Dispatches between Constructor, Subplan, RequiredSingleton, and JavaInstance types.
 * It is used e.g. in Walk.preorder()
 */
public abstract class AbstractInjectionPlanNodeVisitor implements NodeVisitor<InjectionPlan<?>> {

  /**
   * Manually dispatch between different types of injection plan objects and call proper
   * visit() method. Currently dispatches between Constructor, Subplan, RequiredSingleton,
   * and JavaInstance types.
   * @param node TANG injection plan node.
   * @return true to proceed with the next node, false to cancel.
   * @throws ClassCastException if argument is not one of Constructor, Subplan,
   * RequiredSingleton, or JavaInstance.
   */
  @Override
  public boolean visit(final InjectionPlan<?> node) {
    if (node instanceof Constructor<?>) {
      return visit((Constructor<?>) node);
    } else if (node instanceof Subplan<?>) {
      return visit((Subplan<?>) node);
    } else if (node instanceof JavaInstance<?>) {
      return visit((JavaInstance<?>) node);
    }
    throw new ClassCastException(
        "Node " + node.getClass() + " cannot be casted to one of the known subclasses."
        + " Override this method to handle the case.");
  }

  /**
   * Process current injection plan node of Constructor type.
   * @param node Current injection plan node.
   * @return true to proceed with the next node, false to cancel.
   */
  public abstract boolean visit(Constructor<?> node);

  /**
   * Process current injection plan node of JavaInstance type.
   * @param node Current injection plan node.
   * @return true to proceed with the next node, false to cancel.
   */
  public abstract boolean visit(JavaInstance<?> node);

  /**
   * Process current injection plan node of Subplan type.
   * @param node Current injection plan node.
   * @return true to proceed with the next node, false to cancel.
   */
  public abstract boolean visit(Subplan<?> node);
}

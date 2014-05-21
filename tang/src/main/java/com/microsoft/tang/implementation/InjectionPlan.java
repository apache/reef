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
import com.microsoft.tang.types.Traversable;

import java.util.Collection;
import java.util.Collections;

public abstract class InjectionPlan<T> implements Traversable<InjectionPlan<?>> {

  protected final Node node;

  public InjectionPlan(final Node node) {
    this.node = node;
  }

  private static void newline(StringBuffer pretty, int indent) {
    pretty.append('\n');
    for (int j = 0; j < indent * 2; j++) {
      pretty.append(' ');
    }
  }

  public Node getNode() {
    return node;
  }

  /**
   * Get child elements of the injection plan tree.
   * By default, returns an empty list.
   *
   * @return An empty list.
   */
  @SuppressWarnings("unchecked")
  @Override
  public Collection<InjectionPlan<?>> getChildren() {
    return Collections.EMPTY_LIST;
  }

  public abstract int getNumAlternatives();

  public boolean isFeasible() {
    return getNumAlternatives() > 0;
  }

  abstract public boolean isAmbiguous();

  abstract public boolean isInjectable();

  protected void pad(StringBuffer sb, int n) {
    for (int i = 0; i < n; i++) {
      sb.append("  ");
    }
  }

  public String toPrettyString() {
    String ugly = node.getFullName() + ":\n" + toString();
    StringBuffer pretty = new StringBuffer();
    int currentIndent = 1;
    for (int i = 0; i < ugly.length(); i++) {
      char c = ugly.charAt(i);
      if (c == '(') {
        if (ugly.charAt(i + 1) == ')') {
          pretty.append("()");
          i++;
        } else {
          newline(pretty, currentIndent);
          currentIndent++;
          pretty.append(c);
          pretty.append(' ');
        }
      } else if (c == '[') {
        if (ugly.charAt(i + 1) == ']') {
          pretty.append("[]");
          i++;
        } else {
          newline(pretty, currentIndent);
          currentIndent++;
          pretty.append(c);
          pretty.append(' ');
        }
      } else if (c == ')' || c == ']') {
        currentIndent--;
        newline(pretty, currentIndent);
        pretty.append(c);
      } else if (c == '|') {
        newline(pretty, currentIndent);
        pretty.append(c);
      } else if (c == ',') {
        currentIndent--;
        newline(pretty, currentIndent);
        pretty.append(c);
        currentIndent++;
      } else {
        pretty.append(c);
      }
    }
    return pretty.toString();
  }

  /**
   * Algorithm for generating cant inject string:
   * <p/>
   * For infeasible plans:
   * <p/>
   * Some node types are "leaves":
   * <ul>
   * <li>NamedParameterNode</li>
   * <li>ClassNode with no @Inject constructors</li>
   * </ul>
   * We do a depth first search of the injection plan, starting with the left
   * most constructor arguments. When we encounter a constructor whose arguments
   * are all either injectable or non-injectable leaf nodes, we return the name
   * of its parent, and the name of the non-injectable leaves.
   * <p/>
   * For ambiguous plans:
   * <p/>
   * We perform a depth first search of the ambiguous constructors, as above. We
   * return the name of the first class that has multiple constructors that are
   * feasible or ambiguous (as opposed to having a single constructor with an
   * ambiguous argument, or a constructor with an infeasible argument and an
   * ambiguous argument).
   */
  public final String toCantInjectString() {
    if (!isFeasible()) {
      return toInfeasibleInjectString();
    } else if (isAmbiguous()) {
      return toAmbiguousInjectString();
    } else {
      throw new IllegalArgumentException(
          "toCantInjectString() called on injectable constructor:"
              + this.toPrettyString());
    }
  }

  protected abstract String toAmbiguousInjectString();

  protected abstract String toInfeasibleInjectString();

  protected abstract boolean isInfeasibleLeaf();

  public abstract String toShallowString();
}

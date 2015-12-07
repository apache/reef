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
package org.apache.reef.tang.implementation.types;

import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.types.ClassNode;
import org.apache.reef.tang.types.ConstructorArg;
import org.apache.reef.tang.types.ConstructorDef;

import java.util.Arrays;

public class ConstructorDefImpl<T> implements ConstructorDef<T> {
  private final ConstructorArg[] args;
  private final String className;

  public ConstructorDefImpl(final String className, final ConstructorArg[] args,
                            final boolean injectable) throws ClassHierarchyException {
    this.className = className;
    this.args = args;
    if (injectable) {
      for (int i = 0; i < this.getArgs().length; i++) {
        for (int j = i + 1; j < this.getArgs().length; j++) {
          if (this.getArgs()[i].equals(this.getArgs()[j])) {
            throw new ClassHierarchyException(
                "Repeated constructor parameter detected.  "
                    + "Cannot inject constructor " + toString());
          }
        }
      }
    }
  }

  @Override
  public ConstructorArg[] getArgs() {
    return args;
  }

  @Override
  public String getClassName() {
    return className;
  }

  private String join(final String sep, final Object[] vals) {
    if (vals.length != 0) {
      final StringBuilder sb = new StringBuilder(vals[0].toString());
      for (int i = 1; i < vals.length; i++) {
        sb.append(sep + vals[i]);
      }
      return sb.toString();
    } else {
      return "";
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(className);
    sb.append("(");
    sb.append(join(",", args));
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean takesParameters(final ClassNode<?>[] paramTypes) {
    if (paramTypes.length != args.length) {
      return false;
    }
    for (int i = 0; i < paramTypes.length; i++) {
      if (!args[i].getType().equals(paramTypes[i].getFullName())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check to see if two boundConstructors take indistinguishable arguments. If
   * so (and they are in the same class), then this would lead to ambiguous
   * injection targets, and we want to fail fast.
   * <p>
   * TODO could be faster. Currently O(n^2) in number of parameters.
   *
   * @param def
   * @return
   */
  private boolean equalsIgnoreOrder(final ConstructorDef<?> def) {
    if (getArgs().length != def.getArgs().length) {
      return false;
    }
    for (int i = 0; i < getArgs().length; i++) {
      boolean found = false;
      for (int j = 0; j < def.getArgs().length; j++) {
        if (getArgs()[i].getName().equals(def.getArgs()[j].getName())) {
          found = true;
        }
      }
      if (!found) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return equalsIgnoreOrder((ConstructorDef<?>) o);
  }

  @Override
  public boolean isMoreSpecificThan(final ConstructorDef<?> def) {
    // Return true if our list of args is a superset of those in def.

    // Is everything in def also in this?
    for (int i = 0; i < def.getArgs().length; i++) {
      boolean found = false;
      for (int j = 0; j < this.getArgs().length; j++) {
        if (getArgs()[j].equals(def.getArgs()[i])) {
          found = true;
          break;
        }
      }
      // If not, then argument j from def is not in our list.  Return false.
      if (!found) {
        return false;
      }
    }
    // Everything in def's arg list is in ours.  Do we have at least one extra
    // argument?
    return getArgs().length > def.getArgs().length;
  }

  @Override
  public int compareTo(final ConstructorDef<?> o) {
    return toString().compareTo(o.toString());
  }

  @Override
  public int hashCode() {
    final ConstructorArg[] argsSort = getArgs().clone();
    Arrays.sort(argsSort);
    int result = Arrays.hashCode(argsSort);
    result = 31 * result + (this.className == null ? 0 : this.className.hashCode());
    return result;
  }
}

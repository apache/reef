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
package com.microsoft.tang.implementation.types;

import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.ConstructorDef;

public class ConstructorDefImpl<T> implements ConstructorDef<T> {
  private final ConstructorArg[] args;
  private final String className;

  @Override
  public ConstructorArg[] getArgs() {
    return args;
  }

  @Override
  public String getClassName() {
    return className;
  }

  private String join(String sep, Object[] vals) {
    if (vals.length != 0) {
      StringBuilder sb = new StringBuilder(vals[0].toString());
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
    StringBuilder sb = new StringBuilder(className);
    sb.append("(");
    sb.append(join(",", args));
    sb.append(")");
    return sb.toString();
  }

  public ConstructorDefImpl(String className, ConstructorArg[] args,
      boolean injectable) throws ClassHierarchyException {
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
  public boolean takesParameters(ClassNode<?>[] paramTypes) {
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
   * 
   * TODO could be faster. Currently O(n^2) in number of parameters.
   * 
   * @param def
   * @return
   */
  private boolean equalsIgnoreOrder(ConstructorDef<?> def) {
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
  public boolean equals(Object o) {
    return equalsIgnoreOrder((ConstructorDef<?>) o);
  }

  @Override
  public boolean isMoreSpecificThan(ConstructorDef<?> def) {
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
      if (found == false)
        return false;
    }
    // Everything in def's arg list is in ours.  Do we have at least one extra
    // argument?
    return getArgs().length > def.getArgs().length;
  }

  @Override
  public int compareTo(ConstructorDef<?> o) {
    return toString().compareTo(o.toString());
  }
}
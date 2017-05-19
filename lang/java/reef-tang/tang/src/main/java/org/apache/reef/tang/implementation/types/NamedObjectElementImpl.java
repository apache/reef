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

import org.apache.reef.tang.types.ClassNode;
import org.apache.reef.tang.types.NamedObjectElement;

/**
 * Default implementation class for NamedObject interface.
 */
public final class NamedObjectElementImpl<T> implements NamedObjectElement<T> {

  private ClassNode<T> typeNode;
  private String name;
  private boolean isNull;
  private Class<T> implementationClass;

  public NamedObjectElementImpl(final ClassNode<T> typeNode,
                                final Class implementationClass,
                                final String name,
                                final boolean isNull) {
    if (isNull) {
      this.typeNode = null;
      this.implementationClass = null;
      this.name = "NULL_NAMED_OBJECT";
    } else {
      this.typeNode = typeNode;
      this.implementationClass = implementationClass;
      this.name = name;
    }
    this.isNull = isNull;
  }

  public boolean isNull() {
    return isNull;
  }

  @Override
  public int compareTo(final NamedObjectElement noe) {
    if (isNull) {
      if (noe.isNull()) {
        return 0;
      } else {
        return 1;
      }
    } else if (noe.isNull()) {
      return -1;
    }
    return getFullName().compareTo(noe.getFullName());
  }

  @Override
  public ClassNode<T> getTypeNode() {
    return typeNode;
  }

  @Override
  public Class<T> getImplementationClass() {
    return implementationClass;
  }

  @Override
  public String getName() {
    return name;
  }

  // TODO: full name should include full name of the type.
  @Override
  public String getFullName() {
    return name;
  }

  @Override
  public int hashCode() {
    if (isNull) {
      return ("NULL:" + getName()).hashCode();
    } else {
      return ("NOTNULL:" + getName()).hashCode();
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NamedObjectElementImpl noe = (NamedObjectElementImpl) o;
    if (isNull) {
      return noe.isNull;
    }
    return this.typeNode.equals(noe.typeNode) && (this.name.equals(noe.name));
  }

  @Override
  public String toString() {
    if (isNull) {
      return String.format("[%s]", getName());
    } else {
      return String.format("[%s '%s]", this.getClass().getSimpleName(), getName());
    }
  }
}

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

import org.apache.reef.tang.types.ConstructorArg;

public class ConstructorArgImpl implements ConstructorArg {
  private final String type;
  private final String name;
  private final boolean isInjectionFuture;

  public ConstructorArgImpl(final String type, final String namedParameterName, final boolean isInjectionFuture) {
    this.type = type;
    this.name = namedParameterName;
    this.isInjectionFuture = isInjectionFuture;
  }

  @Override
  public String getName() {
    return name == null ? type : name;
  }

  @Override
  public String getNamedParameterName() {
    return name;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public String toString() {
    return name == null ? type : type + " " + name;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ConstructorArgImpl arg = (ConstructorArgImpl) o;
    if (!type.equals(arg.type)) {
      return false;
    }
    if (name == null && arg.name == null) {
      return true;
    }
    if (name == null || arg.name == null) {
      return false;
    }
    return name.equals(arg.name);

  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }

  @Override
  public boolean isInjectionFuture() {
    return isInjectionFuture;
  }
}

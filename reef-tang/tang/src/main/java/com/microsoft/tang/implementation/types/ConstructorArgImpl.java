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
package com.microsoft.tang.implementation.types;

import com.microsoft.tang.types.ConstructorArg;

public class ConstructorArgImpl implements ConstructorArg {
  private final String type;
  private final String name;
  private final boolean isInjectionFuture;
  
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

  public ConstructorArgImpl(String type, String namedParameterName, boolean isInjectionFuture) {
    this.type = type;
    this.name = namedParameterName;
    this.isInjectionFuture = isInjectionFuture;
  }

  @Override
  public String toString() {
    return name == null ? type : type + " " + name;
  }

  @Override
  public boolean equals(Object o) {
    ConstructorArgImpl arg = (ConstructorArgImpl) o;
    if (!type.equals(arg.type)) {
      return false;
    }
    if (name == null && arg.name == null) {
      return true;
    }
    if (name == null && arg.name != null) {
      return false;
    }
    if (name != null && arg.name == null) {
      return false;
    }
    return name.equals(arg.name);

  }

  @Override
  public boolean isInjectionFuture() {
    return isInjectionFuture;
  }
}
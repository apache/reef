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
package com.microsoft.tang.test;

import com.microsoft.tang.InjectionFuture;

import javax.inject.Inject;

/**
 * Part of a cyclic dependency.
 */
final class CyclicDependencyClassTwo {
  private final InjectionFuture<CyclicDependencyClassOne> other;

  @Inject
  CyclicDependencyClassTwo(InjectionFuture<CyclicDependencyClassOne> other) {
    this.other = other;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return other.hashCode();
  }
}

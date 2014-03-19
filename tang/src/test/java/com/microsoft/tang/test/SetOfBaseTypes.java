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

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Set;

/**
 * A class that depends on sets of base types.
 */
final class SetOfBaseTypes {
  private final Set<Integer> integers;
  private final Set<Double> doubles;
  private final Set<String> strings;
  private final Set<Integer> moreIntegers;

  @Inject
  SetOfBaseTypes(@Parameter(Integers.class) final Set<Integer> integers,
                 @Parameter(Doubles.class) final Set<Double> doubles,
                 @Parameter(Strings.class) final Set<String> strings,
                 @Parameter(MoreIntegers.class) final Set<Integer> moreIntegers) {
    this.integers = integers;
    this.doubles = doubles;
    this.strings = strings;
    this.moreIntegers = moreIntegers;
  }

  @NamedParameter
  public static class Integers implements Name<Set<Integer>> {
  }

  @NamedParameter(default_values = {"1", "2", "3"})
  public static class MoreIntegers implements Name<Set<Integer>> {
  }

  @NamedParameter
  public static class Doubles implements Name<Set<Double>> {
  }

  @NamedParameter
  public static class Strings implements Name<Set<String>> {
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SetOfBaseTypes that = (SetOfBaseTypes) o;

    if (!doubles.equals(that.doubles)) return false;
    if (!integers.equals(that.integers)) return false;
    if (!strings.equals(that.strings)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = integers.hashCode();
    result = 31 * result + doubles.hashCode();
    result = 31 * result + strings.hashCode();
    return result;
  }
}

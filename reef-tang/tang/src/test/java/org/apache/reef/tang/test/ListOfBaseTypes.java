/**
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
package org.apache.reef.tang.test;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;

final class ListOfBaseTypes {
  private final List<Integer> integers;
  private final List<Double> doubles;
  private final List<String> strings;
  private final List<Integer> moreIntegers;

  @Inject
  ListOfBaseTypes(@Parameter(Integers.class) final List<Integer> integers,
                  @Parameter(Doubles.class) final List<Double> doubles,
                  @Parameter(Strings.class) final List<String> strings,
                  @Parameter(MoreIntegers.class) final List<Integer> moreIntegers) {
    this.integers = integers;
    this.doubles = doubles;
    this.strings = strings;
    this.moreIntegers = moreIntegers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ListOfBaseTypes that = (ListOfBaseTypes) o;

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

  @NamedParameter
  public static class Integers implements Name<List<Integer>> {
  }

  @NamedParameter(default_values = {"1", "2", "3"})
  public static class MoreIntegers implements Name<List<Integer>> {
  }

  @NamedParameter
  public static class Doubles implements Name<List<Double>> {
  }

  @NamedParameter
  public static class Strings implements Name<List<String>> {
  }
}
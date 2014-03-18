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

  @Inject
  SetOfBaseTypes(@Parameter(Integers.class) final Set<Integer> integers,
                 @Parameter(Doubles.class) final Set<Double> doubles,
                 @Parameter(Strings.class) final Set<String> strings) {
    this.integers = integers;
    this.doubles = doubles;
    this.strings = strings;
  }

  @NamedParameter
  public static class Integers implements Name<Set<Integer>> {
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

package com.microsoft.tang.test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

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
}
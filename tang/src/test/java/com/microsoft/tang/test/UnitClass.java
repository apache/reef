package com.microsoft.tang.test;

import com.microsoft.tang.annotations.Unit;

import javax.inject.Inject;

/**
 * A test user for the @Unit annotation
 */
@Unit
final class UnitClass {
  private String stringValue;
  private int intValue;

  @Inject
  UnitClass() {

  }

  public String getStringValue() {
    return stringValue;
  }

  public int getIntValue() {
    return intValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    UnitClass unitClass = (UnitClass) o;

    if (intValue != unitClass.intValue) return false;
    if (stringValue != null ? !stringValue.equals(unitClass.stringValue) : unitClass.stringValue != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = stringValue != null ? stringValue.hashCode() : 0;
    result = 31 * result + intValue;
    return result;
  }

  final class IntegerHandler implements Handler<Integer> {
    final int foo = 42;

    @Override
    public void process(final Integer value) {
      UnitClass.this.intValue = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IntegerHandler that = (IntegerHandler) o;

      if (foo != that.foo) return false;

      return true;
    }

    @Override
    public int hashCode() {
      return foo;
    }
  }

  final class StringHandler implements Handler<String> {
    final int bar = -42;

    @Override
    public void process(final String value) {
      UnitClass.this.stringValue = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StringHandler that = (StringHandler) o;

      if (bar != that.bar) return false;

      return true;
    }

    @Override
    public int hashCode() {
      return bar;
    }
  }


}

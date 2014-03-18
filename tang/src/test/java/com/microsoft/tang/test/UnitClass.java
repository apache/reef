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

  final class IntegerHandler implements Handler<Integer> {

    @Override
    public void process(final Integer value) {
      UnitClass.this.intValue = value;
    }
  }

  final class StringHandler implements Handler<String> {

    @Override
    public void process(final String value) {
      UnitClass.this.stringValue = value;
    }
  }


}

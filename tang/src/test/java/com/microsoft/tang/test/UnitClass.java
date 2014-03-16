package com.microsoft.tang.test;

import com.microsoft.tang.annotations.Unit;

/**
 * A test user for the @Unit annotation
 */
@Unit
public final class UnitClass {

  private static int instanceCounter = 0;

  public UnitClass() {
    ++instanceCounter;
  }

  public final class InnerClassOne implements TypedInterface<Integer> {

    @Override
    public void process(final Integer value) {
      // just do nothing
    }
  }

  public final class InnerClassTwo implements TypedInterface<String> {

    @Override
    public void process(final String value) {
      // just do nothing
    }
  }


}

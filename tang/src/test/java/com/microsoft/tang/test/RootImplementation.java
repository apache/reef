package com.microsoft.tang.test;


import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * The root of the object graph instantiated.
 */
public class RootImplementation implements RootInterface {

  private final String requiredString;
  private final String optionalString;
  private final UnitClass unit;
  private final Handler<String> stringHandler;
  private final Handler<Integer> integerHandler;
  private final AnInterface anInterface;
  private final int anInt;
  private final double aDouble;

  @Inject
  public RootImplementation(@Parameter(TestConfiguration.RequiredString.class) final String requiredString,
                            @Parameter(TestConfiguration.OptionalString.class) final String optionalString,
                            @Parameter(TestConfiguration.StringHandler.class) final Handler<String> stringHandler,
                            @Parameter(TestConfiguration.IntegerHandler.class) final Handler<Integer> integerHandler,
                            @Parameter(TestConfiguration.NamedParameterInteger.class) final int anInt,
                            @Parameter(TestConfiguration.NamedParameterDouble.class) double aDouble,
                            final UnitClass unit, AnInterface anInterface) {
    this.requiredString = requiredString;
    this.optionalString = optionalString;
    this.unit = unit;
    this.stringHandler = stringHandler;
    this.integerHandler = integerHandler;
    this.anInterface = anInterface;
    this.anInt = anInt;
    this.aDouble = aDouble;
  }

  @Override
  public boolean isValid() {
    if (!this.requiredString.equals(TestConfiguration.REQUIRED_STRING_VALUE)) {
      return false;
    }

    if (!this.optionalString.equals(TestConfiguration.OPTIONAL_STRING_VALUE)) {
      return false;
    }

    this.integerHandler.process(3);
    this.stringHandler.process("three");
    if (this.unit.getIntValue() != 3) {
      return false;
    }
    if (!this.unit.getStringValue().equals("three")) {
      return false;
    }
    if (this.anInterface == null) {
      return false;
    }

    if (this.aDouble != TestConfiguration.NAMED_PARAMETER_DOUBLE_VALUE) {
      return false;
    }
    if (this.anInt != TestConfiguration.NAMED_PARAMETER_INTEGER_VALUE) {
      return false;
    }

    return true;
  }
}

package com.microsoft.tang.test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.tang.formats.RequiredParameter;

import java.util.Set;

/**
 * All the configuration parameters and options for the test without list
 */
public class TestConfigurationWithoutList extends ConfigurationModuleBuilder {
  // TODO: Remove this method after #192 is fixed
  @NamedParameter()
  public static final class RequiredString implements Name<String> {
  }

  public static final String REQUIRED_STRING_VALUE = "Required String Value";

  @NamedParameter(default_value = "default_string_default_value")
  public static final class OptionalString implements Name<String> {
  }

  public static final String OPTIONAL_STRING_VALUE = "Optional String Value";

  public static final RequiredParameter<String> REQUIRED_STRING = new RequiredParameter<>();

  public static final OptionalParameter<String> OPTIONAL_STRING = new OptionalParameter<>();

  @NamedParameter()
  public static final class IntegerHandler implements Name<Handler<Integer>> {
  }

  @NamedParameter()
  public static final class StringHandler implements Name<Handler<String>> {
  }

  @NamedParameter()
  public static final class NamedParameterInteger implements Name<Integer> {
  }

  public static final int NAMED_PARAMETER_INTEGER_VALUE = 42;

  @NamedParameter()
  public static final class NamedParameterDouble implements Name<Double> {
  }

  public static final double NAMED_PARAMETER_DOUBLE_VALUE = 42.0;

  @NamedParameter
  public static final class SetOfInstances implements Name<Set<SetInterface>> {
  }

  public static final ConfigurationModule CONF = new TestConfigurationWithoutList()
      .bindImplementation(RootInterface.class, RootImplementationWithoutList.class)
      .bindNamedParameter(IntegerHandler.class, UnitClass.IntegerHandler.class)
      .bindNamedParameter(StringHandler.class, UnitClass.StringHandler.class)
      .bindNamedParameter(NamedParameterInteger.class, String.valueOf(NAMED_PARAMETER_INTEGER_VALUE))
      .bindNamedParameter(NamedParameterDouble.class, String.valueOf(NAMED_PARAMETER_DOUBLE_VALUE))
      .bindSetEntry(SetOfInstances.class, SetInterfaceImplOne.class)
      .bindSetEntry(SetOfInstances.class, SetInterfaceImplTwo.class)
          // Adds list implementations
      .bindNamedParameter(RequiredString.class, REQUIRED_STRING)
      .bindNamedParameter(OptionalString.class, OPTIONAL_STRING)
          // Sets of base types
      .bindSetEntry(SetOfBaseTypes.Integers.class, "1")
      .bindSetEntry(SetOfBaseTypes.Integers.class, "2")
      .bindSetEntry(SetOfBaseTypes.Integers.class, "3")
      .bindSetEntry(SetOfBaseTypes.Doubles.class, "1")
      .bindSetEntry(SetOfBaseTypes.Doubles.class, "2")
      .bindSetEntry(SetOfBaseTypes.Doubles.class, "3")
      .bindSetEntry(SetOfBaseTypes.Strings.class, "1")
      .bindSetEntry(SetOfBaseTypes.Strings.class, "2")
      .bindSetEntry(SetOfBaseTypes.Strings.class, "3")
          // Lists of base types
      .build();
}

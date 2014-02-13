package com.microsoft.reef.tests.evaluatorsize;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;

public final class EvaluatorSizeTestConfiguration extends ConfigurationModuleBuilder {

  @NamedParameter(doc = "The size of the Evaluator to test for")
  public static class MemorySize implements Name<Integer> {
  }

  public static final RequiredParameter<Integer> MEMORY_SIZE = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new EvaluatorSizeTestConfiguration()
      .bindNamedParameter(MemorySize.class, MEMORY_SIZE)
      .build();

}

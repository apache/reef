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

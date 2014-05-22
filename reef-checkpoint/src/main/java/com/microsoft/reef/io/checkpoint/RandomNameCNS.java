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
package com.microsoft.reef.io.checkpoint;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import org.apache.commons.lang.RandomStringUtils;

import javax.inject.Inject;

/**
 * Simple naming service that generates a random checkpoint name.
 */
public class RandomNameCNS implements CheckpointNamingService {

  private final String prefix;

  @NamedParameter(doc = "The prefix used for the random names returned.", default_value = "checkpoint_")
  public static class PREFIX implements Name<String> {
  }

  @Inject
  public RandomNameCNS(@Parameter(PREFIX.class) final String prefix) {
    this.prefix = prefix;
  }

  @Override
  public String getNewName() {
    return this.prefix + RandomStringUtils.randomAlphanumeric(8);
  }

}

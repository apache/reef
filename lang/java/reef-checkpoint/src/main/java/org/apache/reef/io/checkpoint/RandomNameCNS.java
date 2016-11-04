/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.io.checkpoint;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A naming service that generates a random checkpoint name by appending a random alphanumeric string (suffix)
 * of a given length to a user-supplied prefix string.
 */
public class RandomNameCNS implements CheckpointNamingService {

  private final String prefix;
  private final int lengthOfRandomSuffix;

  @Deprecated
  public RandomNameCNS(@Parameter(PREFIX.class) final String prefix) {
    this.prefix = prefix;
    this.lengthOfRandomSuffix
            = Integer.parseInt(LengthOfRandomSuffix.class.getAnnotation(NamedParameter.class).default_value());
  }

  @Inject
  private RandomNameCNS(@Parameter(PREFIX.class) final String prefix,
                        @Parameter(LengthOfRandomSuffix.class) final int lengthOfRandomSuffix) {
    this.prefix = prefix;
    this.lengthOfRandomSuffix = lengthOfRandomSuffix;
  }

  @Override
  public String getNewName() {
    return this.prefix + RandomStringUtils.randomAlphanumeric(lengthOfRandomSuffix);
  }

  /**
   * The prefix used for the random names returned.
   */
  @NamedParameter(doc = "The prefix used for the random names returned.", default_value = "checkpoint_")
  public static class PREFIX implements Name<String> {
  }

  /**
   * Number of alphanumeric characters in the random part of a checkpoint name.
   */
  @NamedParameter(doc = "Number of alphanumeric chars in the random part of a checkpoint name.", default_value = "8")
  public static class LengthOfRandomSuffix implements Name<Integer> {
  }

}

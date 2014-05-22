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

import javax.inject.Inject;

/**
 * A naming service that simply returns the name it has been initialized with.
 */
public class SimpleNamingService implements CheckpointNamingService {

  /**
   * Prefix for checkpoints.
   */
  @NamedParameter(doc = "Checkpoint prefix.", short_name = "checkpoint_prefix", default_value = "reef")
  public static final class CheckpointName implements Name<String> {
  }

  private final String name;

  @Inject
  public SimpleNamingService(@Parameter(CheckpointName.class) final String name) {
    this.name = "checkpoint_" + name;
  }

  /**
   * Generate a new checkpoint Name.
   *
   * @return the checkpoint name
   */
  @Override
  public String getNewName() {
    return this.name;
  }
}

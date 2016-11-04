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

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A naming service that simply returns the name it has been initialized with.
 * Note that the name is always the same.
 */
public class SimpleNamingService implements CheckpointNamingService {

  private final String name;

  @Inject
  public SimpleNamingService(@Parameter(CheckpointName.class) final String name) {
    this.name = "checkpoint_" + name;
  }

  /**
   * Generate a new checkpoint name.
   *
   * @return the checkpoint name
   */
  @Override
  public String getNewName() {
    return this.name;
  }

  /**
   * Prefix for checkpoints.
   */
  @NamedParameter(doc = "Checkpoint name.", short_name = "checkpoint_name", default_value = "reef")
  public static final class CheckpointName implements Name<String> {
  }
}

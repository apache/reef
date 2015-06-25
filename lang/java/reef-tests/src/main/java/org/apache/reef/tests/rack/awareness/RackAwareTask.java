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
package org.apache.reef.tests.rack.awareness;

import org.apache.commons.lang.Validate;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;
import java.util.logging.Logger;

import javax.inject.Inject;

/**
 * A simple task that receives the rack it is executed on
 */
public final class RackAwareTask implements Task {

  private static final Logger LOG = Logger.getLogger(RackAwareTask.class.getName());

  private final String rackName;

  @Inject
  RackAwareTask(@Parameter(RackNameParameter.class) final String rackName) {
    Validate.notEmpty(rackName);
    this.rackName = rackName;
  }

  @Override
  public final byte[] call(final byte[] memento) {
    LOG.info("Evaluator executing in " + rackName);
    return null;
  }
}

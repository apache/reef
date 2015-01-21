/**
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
package org.apache.reef.runtime.common.client.defaults;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.FailedRuntime;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default event handler for REEF FailedRuntime: rethrow the exception.
 */
@Provided
@ClientSide
public final class DefaultRuntimeErrorHandler implements EventHandler<FailedRuntime> {

  private static final Logger LOG = Logger.getLogger(DefaultRuntimeErrorHandler.class.getName());

  @Inject
  private DefaultRuntimeErrorHandler() {
  }

  @Override
  public void onNext(final FailedRuntime error) {
    if (error.getReason().isPresent()) {
      LOG.log(Level.SEVERE, "Runtime error: " + error, error.getReason().get());
    } else {
      LOG.log(Level.SEVERE, "Runtime error: " + error);
    }
    throw new RuntimeException("REEF runtime error: " + error, error.asError());
  }
}

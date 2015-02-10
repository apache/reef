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
package org.apache.reef.tests.multipleEventHandlerInstances;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The ActiveContext handler
 */
public final class ActiveContextHandler implements EventHandler<ActiveContext> {

  private static final Logger LOG = Logger.getLogger(ActiveContextHandler.class.getName());

  private static int countInstances = 0;

  @Inject
  public ActiveContextHandler() {
    ++countInstances;
    if (countInstances > 1) {
      throw new DriverSideFailure("Expect ActiveContextHandler to be created only once");
    }
  }

  @Override
  public void onNext(ActiveContext activeContext) {
    LOG.log(Level.FINEST, "ActiveContext received. Submitting empty task to it");
    Configuration taskConfiguration;
    try {
      taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "EmptyREEFTask")
          .set(TaskConfiguration.TASK, EmptyTask.class)
          .build();
    } catch (BindException e) {
      throw new RuntimeException("Unable to setup Task configuration", e);
    }
    activeContext.submitTask(taskConfiguration);
  }
}

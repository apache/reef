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

package com.microsoft.reef.javabridge;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FailedTaskBridge extends NativeBridge{
  private static final Logger LOG = Logger.getLogger(FailedTaskBridge.class.getName());

  private FailedTask jfailedTask;
  private ActiveContextBridge jactiveContext;

  public FailedTaskBridge(FailedTask failedTask)
  {
    jfailedTask = failedTask;
    Optional<ActiveContext> activeContext = failedTask.getActiveContext();
    jactiveContext = activeContext.isPresent() ? new ActiveContextBridge(activeContext.get()) : null;
  }

  public String getFailedTaskString()
  {
    String description = jfailedTask.getDescription().isPresent() ? jfailedTask.getDescription().get().replace("=", "") : "";
    String cause = jfailedTask.getReason().isPresent() ? jfailedTask.getReason().get().toString().replace("=", "") : "";
    String poorString = "Identifier=" + jfailedTask.getId().replace("=", "")
              + ", Message=" +  jfailedTask.getMessage().replace("=", "")
              + ", Description=" + description
              + ", Cause=" + cause;
    LOG.log(Level.INFO, "serialized failed task " + poorString);
    return  poorString;
  }

  @Override
  public void close()
  {
  }
}


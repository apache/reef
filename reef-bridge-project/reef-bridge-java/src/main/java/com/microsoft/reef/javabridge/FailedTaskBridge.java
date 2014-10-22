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
    final String description = jfailedTask.getDescription().isPresent() ? jfailedTask.getDescription().get().replace("=", "").replace(",", "") : "";
    final String cause = jfailedTask.getReason().isPresent() ? jfailedTask.getReason().get().toString().replace("=", "").replace(",", "") : "";
    final String data = jfailedTask.getData().isPresent() ? new String(jfailedTask.getData().get()).replace("=", "").replace(",", "") : "";

    // TODO: deserialize/serialize with proper Avro schema
    final String poorSerializedString = "Identifier=" + jfailedTask.getId().replace("=", "").replace(",", "")
      + ", Message=" +  jfailedTask.getMessage().replace("=", "").replace(",", "")
      + ", Description=" + description
      + ", Cause=" + cause
      + ", Data=" + data;

    LOG.log(Level.INFO, "serialized failed task " + poorSerializedString);
    return  poorSerializedString;
  }

  @Override
  public void close()
  {
  }
}


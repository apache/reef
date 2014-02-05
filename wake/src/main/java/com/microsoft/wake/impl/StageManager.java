/**
 * Copyright (C) 2012 Microsoft Corporation
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
package com.microsoft.wake.impl;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.wake.Stage;


/**
 * A manager that manages all the stage
 */
public final class StageManager implements Stage {

  private static final Logger LOG = Logger.getLogger(StageManager.class.getName());

  private static StageManager instance = new StageManager();
  private final List<Stage> stages;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  
  StageManager() {

    stages = Collections.synchronizedList(new ArrayList<Stage>());
    LOG.log(Level.FINE, "StageManager adds a shutdown hook");
    Runtime.getRuntime().addShutdownHook(new Thread(
      new Runnable() {
        @Override
        public void run() {
          try {
            LOG.log(Level.FINEST, "Shutdown hook : closing stages");
            StageManager.instance().close();
            LOG.log(Level.FINEST, "Shutdown hook : closed stages");
          } catch (Exception e) {
            LOG.log(Level.WARNING, "StageManager close failure " + e.getMessage());
          }
        }
      }
    ));
  }
  
  public static StageManager instance() {
    return instance;
  }
  
  public void register(Stage stage) {
    LOG.log(Level.FINEST, "StageManager adds stage " + stage);

    stages.add(stage);
  }
  
  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      for (Stage stage : stages) {
        LOG.log(Level.FINEST, "Closing {0}", stage);
        stage.close();
      }
    }
  }
 
}

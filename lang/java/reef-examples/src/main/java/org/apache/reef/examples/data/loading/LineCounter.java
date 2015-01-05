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
package org.apache.reef.examples.data.loading;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.poison.PoisonedConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver side for the line counting demo that uses the data loading service.
 */
@DriverSide
@Unit
public class LineCounter {

  private static final Logger LOG = Logger.getLogger(LineCounter.class.getName());

  private final AtomicInteger ctrlCtxIds = new AtomicInteger();
  private final AtomicInteger lineCnt = new AtomicInteger();
  private final AtomicInteger completedDataTasks = new AtomicInteger();

  private final DataLoadingService dataLoadingService;

  @Inject
  public LineCounter(final DataLoadingService dataLoadingService) {
    this.dataLoadingService = dataLoadingService;
    this.completedDataTasks.set(dataLoadingService.getNumberOfPartitions());
  }

  public class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {

      final String contextId = activeContext.getId();
      LOG.log(Level.FINER, "Context active: {0}", contextId);

      if (dataLoadingService.isDataLoadedContext(activeContext)) {

        final String lcContextId = "LineCountCtxt-" + ctrlCtxIds.getAndIncrement();
        LOG.log(Level.FINEST, "Submit LineCount context {0} to: {1}",
            new Object[]{lcContextId, contextId});

        final Configuration poisonedConfiguration = PoisonedConfiguration.CONTEXT_CONF
            .set(PoisonedConfiguration.CRASH_PROBABILITY, "0.4")
            .set(PoisonedConfiguration.CRASH_TIMEOUT, "1")
            .build();

        activeContext.submitContext(Tang.Factory.getTang()
            .newConfigurationBuilder(poisonedConfiguration,
                ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER, lcContextId).build())
            .build());

      } else if (activeContext.getId().startsWith("LineCountCtxt")) {

        final String taskId = "LineCountTask-" + ctrlCtxIds.getAndIncrement();
        LOG.log(Level.FINEST, "Submit LineCount task {0} to: {1}", new Object[]{taskId, contextId});

        try {
          activeContext.submitTask(TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, taskId)
              .set(TaskConfiguration.TASK, LineCountingTask.class)
              .build());
        } catch (final BindException ex) {
          LOG.log(Level.SEVERE, "Configuration error in " + contextId, ex);
          throw new RuntimeException("Configuration error in " + contextId, ex);
        }
      } else {
        LOG.log(Level.FINEST, "Line count Compute Task {0} -- Closing", contextId);
        activeContext.close();
      }
    }
  }

  public class TaskCompletedHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completedTask) {

      final String taskId = completedTask.getId();
      LOG.log(Level.FINEST, "Completed Task: {0}", taskId);

      final byte[] retBytes = completedTask.get();
      final String retStr = retBytes == null ? "No RetVal" : new String(retBytes);
      LOG.log(Level.FINE, "Line count from {0} : {1}", new String[]{taskId, retStr});

      lineCnt.addAndGet(Integer.parseInt(retStr));

      if (completedDataTasks.decrementAndGet() <= 0) {
        LOG.log(Level.INFO, "Total line count: {0}", lineCnt.get());
      }

      LOG.log(Level.FINEST, "Releasing Context: {0}", taskId);
      completedTask.getActiveContext().close();
    }
  }
}

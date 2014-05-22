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
package com.microsoft.reef.examples.data.loading;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver side for the line counting
 * demo that uses the data loading service
 */
@DriverSide
@Unit
public class LineCounter {

  private static final Logger LOG = Logger.getLogger(LineCounter.class.getName());
  
  private final DataLoadingService dataLoadingService;

  private final AtomicInteger ctrlCtxIds = new AtomicInteger();
  
  private final AtomicInteger completedDataTasks;
  
  private AtomicInteger lineCnt = new AtomicInteger();

  @Inject
  public LineCounter(DataLoadingService dataLoadingService) {
    super();
    this.dataLoadingService = dataLoadingService;
    this.completedDataTasks = new AtomicInteger(dataLoadingService.getNumberOfPartitions());
  }

  /**
   *
   */
  public class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask completedTask) {
      final String taskId = completedTask.getId();
      LOG.log(Level.INFO, "Completed Task: " + taskId);
      final byte[] retVal = completedTask.get();
      final String retStr = new String(retVal == null ? "No RetVal".getBytes() : retVal);
      LOG.log(Level.INFO, "Line count from " + taskId + " " + retStr);
      lineCnt.addAndGet(Integer.parseInt(retStr));
      if(completedDataTasks.decrementAndGet()==0)
        LOG.log(Level.INFO, "Total line count: " + lineCnt.get());
      LOG.log(Level.INFO, "Releasing Context: " + taskId);
      completedTask.getActiveContext().close();
    }
  }

  /**
   *
   */
  public class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {
      if(dataLoadingService.isDataLoadedContext(activeContext)){
        final String evaluatorId = activeContext.getEvaluatorId();
        final String taskId = "LineCount-" + ctrlCtxIds.getAndIncrement();
        try {
          final Configuration taskConfiguration = TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, taskId)
              .set(TaskConfiguration.TASK, LineCountingTask.class)
              .build();
          activeContext.submitTask(taskConfiguration);
        } catch (BindException e) {
          throw new RuntimeException("Unable to create context/task configuration for " + evaluatorId, e);
        }
      }
      else{
        LOG.log(Level.INFO, "Line count Compute Task " + activeContext.getId() + 
            " -- Closing");
        activeContext.close();
        return;
      }
    }
  }
}

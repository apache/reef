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
package org.apache.reef.runtime.spark;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;


@Unit
final class ReefOnSparkDriver  {

    //@Inject
    //private(val requestor: EvaluatorRequestor)
  private Logger LOG = Logger.getLogger(ReefOnSparkDriver.getClass().getName());

  LOG.log(Level.FINE, "Instantiated ReefOnSparkDriver");

  final class StartHandler extends EventHandler[StartTime] {

    public void onNext(StartTime startTime) {
      ReefOnSparkDriver.LOG.log(Level.INFO, "Start ReefOnSparkDriver: {0}", startTime);
      requestor.newRequest
        .setNumber(1)
        .setMemory(64)
        .setNumberOfCores(1)
        .submit();
      ReefOnSparkDriver.LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }


  final class EvaluatorAllocatedHandler extends EventHandler[AllocatedEvaluator] {

          public void onNext(AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO,"Submitting ReefOnSparkTask task to AllocatedEvaluator: {0}", allocatedEvaluator);
      Configuration taskConfiguration = TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, "ReefOnSparkTask")
        .set(TaskConfiguration.TASK, classOf[ReefOnSparkTask])
        .build();
      allocatedEvaluator.submitTask(taskConfiguration);
    }
  }
}

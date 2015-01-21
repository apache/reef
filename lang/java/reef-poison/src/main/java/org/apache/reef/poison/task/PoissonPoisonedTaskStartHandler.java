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
package org.apache.reef.poison.task;

import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.reef.poison.PoisonedAlarmHandler;
import org.apache.reef.poison.params.CrashProbability;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.events.TaskStart;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

final class PoissonPoisonedTaskStartHandler implements EventHandler<TaskStart> {

  private static final Logger LOG = Logger.getLogger(PoissonPoisonedTaskStartHandler.class.getName());

  private final Clock clock;
  private final int timeToCrash;

  @Inject
  public PoissonPoisonedTaskStartHandler(
      final @Parameter(CrashProbability.class) double lambda, final Clock clock) {

    this.clock = clock;
    this.timeToCrash = new PoissonDistribution(lambda * 1000).sample();

    LOG.log(Level.INFO,
        "Created Poisson poison injector with prescribed dose: {0}. Crash in {1} msec.",
        new Object[]{lambda, this.timeToCrash});
  }

  @Override
  public void onNext(final TaskStart taskStart) {
    LOG.log(Level.INFO, "Started Poisson poison injector. Crashing in {0} msec.", this.timeToCrash);
    this.clock.scheduleAlarm(this.timeToCrash, new PoisonedAlarmHandler());
  }
}

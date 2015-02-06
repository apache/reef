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
package org.apache.reef.poison.context;

import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.poison.PoisonException;
import org.apache.reef.poison.PoisonedAlarmHandler;
import org.apache.reef.poison.params.CrashProbability;
import org.apache.reef.poison.params.CrashTimeout;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;

import javax.inject.Inject;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class PoisonedContextStartHandler implements EventHandler<ContextStart> {

  private static final Logger LOG = Logger.getLogger(PoisonedContextStartHandler.class.getName());

  private final Random random = new Random();

  private final double crashProbability;
  private final int timeOut;
  private final Clock clock;

  @Inject
  public PoisonedContextStartHandler(
      final @Parameter(CrashProbability.class) double crashProbability,
      final @Parameter(CrashTimeout.class) int timeOut,
      final Clock clock) {

    this.crashProbability = crashProbability;
    this.timeOut = timeOut;
    this.clock = clock;
  }

  @Override
  public void onNext(final ContextStart contextStart) {

    LOG.log(Level.INFO, "Starting Context poison injector with prescribed dose: {0} units",
        this.crashProbability);

    if (this.random.nextDouble() <= this.crashProbability) {

      final int timeToCrash = this.random.nextInt(this.timeOut) * 1000;
      LOG.log(Level.INFO, "Dosage lethal! Crashing in {0} msec.", timeToCrash);

      if (timeToCrash == 0) {
        throw new PoisonException("Crashed at: " + System.currentTimeMillis());
      } else {
        this.clock.scheduleAlarm(timeToCrash, new PoisonedAlarmHandler());
      }

    } else {
      LOG.info("Dosage not lethal");
    }
  }
}

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
package com.microsoft.reef.poison.context;

import java.util.Random;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.evaluator.context.events.ContextStart;
import com.microsoft.reef.poison.PoisonException;
import com.microsoft.reef.poison.context.params.CrashProbability;
import com.microsoft.reef.poison.context.params.CrashTimeout;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Clock;

final class PoisonedStartHandler implements EventHandler<ContextStart> {

  private static final Logger LOG = Logger.getLogger(PoisonedStartHandler.class.getName());


  private final double crashProbability;
  private final int timeOut;
  private final Clock clock;

  @Inject
  PoisonedStartHandler(final @Parameter(CrashProbability.class) double crashProbability,
                       final @Parameter(CrashTimeout.class) int timeOut,
                       final Clock clock) {
    this.crashProbability = crashProbability;
    this.timeOut = timeOut;
    this.clock = clock;
  }


  @Override
  public void onNext(final ContextStart contextStart) {
    LOG.info("Starting Poison injector with prescribed dose " + crashProbability + " units");
    final Random random = new Random();
    if (random.nextDouble() <= this.crashProbability) {
      LOG.info("Dosage lethal");
      final int timeToCrash = random.nextInt(this.timeOut) * 1000;
      if(timeToCrash==0) {
        LOG.info("Crashing now");
        throw new PoisonException("Crashed at: " + System.currentTimeMillis());
      }
      else {
        LOG.info("Will crash in " + timeToCrash + " secs.");
        this.clock.scheduleAlarm(timeToCrash, new PoisonedAlarmHandler());
      }
    }
    else {
      LOG.info("Dosage not lethal");
    }
  }
}

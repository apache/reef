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

import com.microsoft.reef.evaluator.context.events.ContextStart;
import com.microsoft.reef.poison.PoisonedAlarmHandler;
import com.microsoft.reef.poison.params.CrashProbability;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Clock;

import org.apache.commons.math3.distribution.PoissonDistribution;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

final class PoissonPoisonedContextStartHandler implements EventHandler<ContextStart> {

  private static final Logger LOG = Logger.getLogger(PoissonPoisonedContextStartHandler.class.getName());

  private final Clock clock;
  private final int timeToCrash;

  @Inject
  public PoissonPoisonedContextStartHandler(
      final @Parameter(CrashProbability.class) double lambda, final Clock clock) {

    this.clock = clock;
    this.timeToCrash = new PoissonDistribution(lambda * 1000).sample();

    LOG.log(Level.INFO,
        "Created Poisson poison injector with prescribed dose: {0}. Crash in {1} msec.",
        new Object[] { lambda, this.timeToCrash });
  }

  @Override
  public void onNext(final ContextStart contextStart) {
    LOG.log(Level.INFO, "Started Poisson poison injector. Crashing in {0} msec.", this.timeToCrash);
    this.clock.scheduleAlarm(this.timeToCrash, new PoisonedAlarmHandler());
  }
}

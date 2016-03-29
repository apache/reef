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
package org.apache.reef.tests.evaluatorfailure;

import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A handler for ContextStart that schedules an Alarm handler that throws an Exception.
 */
final class FailureSchedulingContextStartHandler implements EventHandler<ContextStart> {
  private static final Logger LOG = Logger.getLogger(FailureSchedulingContextStartHandler.class.getName());
  private final Clock clock;

  @Inject
  FailureSchedulingContextStartHandler(final Clock clock) {
    this.clock = clock;
  }

  @Override
  // this check is suppressed for all tests, that is, for files which names contain the word "Test"
  // the name of this file does not contain the word, but it still belongs to tests
  @SuppressWarnings("checkstyle:constructorwithoutparams")
  public void onNext(final ContextStart contextStart) {
    this.clock.scheduleAlarm(0, new EventHandler<Alarm>() {
      @Override
      public void onNext(final Alarm alarm) {
        LOG.log(Level.INFO, "Invoked for {0}, throwing an Exception now.", alarm);
        throw new ExpectedException();
      }
    });
  }
}

/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.tests.statepassing;

import com.microsoft.reef.driver.activity.ActivityConfiguration;
import com.microsoft.reef.driver.activity.CompletedActivity;
import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.driver.contexts.ActiveContext;
import com.microsoft.reef.driver.contexts.ContextConfiguration;
import com.microsoft.reef.driver.contexts.ServiceConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public class StatePassingDriver {

  private static final Logger LOG = Logger.getLogger(StatePassingDriver.class.getName());

  private static final int PASSES = 2;

  private int pass = 0;
  private final JobMessageObserver client;

  @Inject
  public StatePassingDriver(final JobMessageObserver client) {
    this.client = client;
  }

  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eb) {
      final JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
      try {
        final Configuration contextConfiguration = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "StatePassingContext")
            .build();

        final Configuration serviceConfiguration = ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, Counter.class)
            .build();

        eb.submitContextAndService(contextConfiguration, serviceConfiguration);
      } catch (final BindException e) {
        throw new RuntimeException(e);
      }
    }
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      nextPass(activeContext);
    }
  }

  private void nextPass(final ActiveContext activeContext) {
    try {
      activeContext.submitActivity(ActivityConfiguration.CONF
          .set(ActivityConfiguration.IDENTIFIER, "StatePassing-" + pass)
          .set(ActivityConfiguration.ACTIVITY, StatePassingActivity.class)
          .build());
      ++pass;
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }

  final class CompletedActivityHandler implements EventHandler<CompletedActivity> {
    @Override
    public void onNext(final CompletedActivity completed) {
      LOG.log(Level.INFO, "Received a completed activity: " + completed);
      final byte[] message = completed.get();

      if (message.length != pass) {
        final String msg = "Expected message of length " + pass + ", but got message of length " + message.length;
        final RuntimeException ex = new RuntimeException(msg);
        client.onError(ex);
        throw ex;
      }
      if (!allEqual((byte) 1, message)) {
        final RuntimeException ex = new RuntimeException("Did not get the right message");
        client.onError(ex);
        throw ex;
      }

      if (pass < PASSES) {
        LOG.log(Level.INFO, "Submitting the next Activity");
        nextPass(completed.getActiveContext());
      } else {
        LOG.log(Level.INFO, "Done");
        completed.getActiveContext().close();
      }
    }
  }

  private static boolean allEqual(final byte value, final byte[] bytes) {
    for (int i = 0; i < bytes.length; ++i) {
      if (bytes[i] != value) {
        return false;
      }
    }
    return true;
  }
}

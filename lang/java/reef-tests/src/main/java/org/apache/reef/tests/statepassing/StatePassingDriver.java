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
package org.apache.reef.tests.statepassing;

import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public class StatePassingDriver {

  private static final Logger LOG = Logger.getLogger(StatePassingDriver.class.getName());

  private static final int PASSES = 2;
  private final JobMessageObserver client;
  private int pass = 0;

  @Inject
  public StatePassingDriver(final JobMessageObserver client) {
    this.client = client;
  }

  private static boolean allEqual(final byte value, final byte[] bytes) {
    for (int i = 0; i < bytes.length; ++i) {
      if (bytes[i] != value) {
        return false;
      }
    }
    return true;
  }

  private void nextPass(final ActiveContext activeContext) {
    try {
      activeContext.submitTask(TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, "StatePassing-" + pass)
          .set(TaskConfiguration.TASK, StatePassingTask.class)
          .build());
      ++pass;
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eb) {
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

  final class ContextActiveHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      nextPass(activeContext);
    }
  }

  final class TaskCompletedHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completed) {
      LOG.log(Level.INFO, "Received a completed task: " + completed);
      final byte[] message = completed.get();

      if (message.length != pass) {
        final String msg = "Expected message of length " + pass + ", but got message of length " + message.length;
        final RuntimeException ex = new RuntimeException(msg);
        throw ex;
      }
      if (!allEqual((byte) 1, message)) {
        final RuntimeException ex = new RuntimeException("Did not get the right message");
        throw ex;
      }

      if (pass < PASSES) {
        LOG.log(Level.INFO, "Submitting the next Task");
        nextPass(completed.getActiveContext());
      } else {
        LOG.log(Level.INFO, "Done");
        completed.getActiveContext().close();
      }
    }
  }
}

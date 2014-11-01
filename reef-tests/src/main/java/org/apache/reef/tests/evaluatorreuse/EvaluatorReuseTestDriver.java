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
package org.apache.reef.tests.evaluatorreuse;

import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tests.library.exceptions.UnexpectedTaskReturnValue;
import org.apache.reef.tests.library.tasks.EchoTask;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import javax.xml.bind.DatatypeConverter;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class EvaluatorReuseTestDriver {

  private static final Logger LOG = Logger.getLogger(EvaluatorReuseTestDriver.class.getName());

  private final int numberOfIterations;
  private final JobMessageObserver client;

  private int counter = 0;
  private String lastMessage = null;

  @Inject
  EvaluatorReuseTestDriver(final @Parameter(NumberOfIterations.class) int n,
                           final JobMessageObserver client) {
    this.numberOfIterations = n;
    this.client = client;
  }

  private void startTask(final ActiveContext context) {
    if (counter < numberOfIterations) {
      try {
        this.lastMessage = "ECHO-" + counter;
        client.sendMessageToClient(("Submitting iteration " + counter).getBytes());
        final String memento = DatatypeConverter.printBase64Binary(this.lastMessage.getBytes());
        context.submitTask(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, this.lastMessage)
            .set(TaskConfiguration.TASK, EchoTask.class)
            .set(TaskConfiguration.MEMENTO, memento)
            .build());
        counter += 1;
      } catch (final BindException e) {
        context.close();
        throw new RuntimeException(e);
      }
    } else {
      client.sendMessageToClient("Done. Closing the Context".getBytes());
      context.close();
    }
  }

  @NamedParameter(default_value = "3", short_name = "i")
  class NumberOfIterations implements Name<Integer> {
  }

  final class TaskCompletedHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completed) {
      final String returned = new String(completed.get());
      final String msg = "CompletedTask returned: \"" + returned + "\"";
      client.sendMessageToClient(msg.getBytes());
      if (!returned.equals(lastMessage)) {
        throw new UnexpectedTaskReturnValue(lastMessage, returned);
      } else {
        startTask(completed.getActiveContext());
      }
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator eb) {
      LOG.log(Level.FINE, "AllocatedEvaluator: " + eb);
      try {
        eb.submitContext(ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "EvaluatorReuse").build());
      } catch (BindException e) {
        throw new RuntimeException(e);
      }
    }
  }

  final class ContextActiveHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      startTask(context);
    }
  }
}

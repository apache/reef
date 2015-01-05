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
package org.apache.reef.tests.subcontexts;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
final class SubContextDriver {

  private static final Logger LOG = Logger.getLogger(SubContextDriver.class.getName());

  private static String CONTEXT_1_IDENTIFIER = "CONTEXT_1";
  private static String CONTEXT_2_IDENTIFIER = "CONTEXT_2";

  private State state = State.INIT; // lock: this

  @Inject
  SubContextDriver() {
  }

  private enum State {
    INIT,
    CONTEXT_1_SUBMITTED,
    CONTEXT_2_SUBMITTED,
    CONTEXT_2_CLOSED,
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {

      LOG.log(Level.FINE, "Submitting root context");

      try {

        final Configuration contextConfiguration = ContextConfiguration.CONF
            .set(ContextConfiguration.ON_CONTEXT_STARTED, ContextStartHandler1.class)
            .set(ContextConfiguration.ON_CONTEXT_STOP, ContextStopHandler1.class)
            .set(ContextConfiguration.IDENTIFIER, CONTEXT_1_IDENTIFIER)
            .build();

        allocatedEvaluator.submitContext(contextConfiguration);

        synchronized (SubContextDriver.this) {
          SubContextDriver.this.state = State.CONTEXT_1_SUBMITTED;
        }

      } catch (final BindException e) {
        throw new RuntimeException(e);
      }
    }
  }

  final class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {

      LOG.log(Level.FINE, "Received ActiveContext: {0}", activeContext);

      if (activeContext.getId().equals(CONTEXT_1_IDENTIFIER)) {

        synchronized (SubContextDriver.this) {
          assert (SubContextDriver.this.state == State.CONTEXT_1_SUBMITTED);
        }

        LOG.log(Level.FINE, "Submitting sub context");

        final Configuration contextConfiguration = ContextConfiguration.CONF
            .set(ContextConfiguration.ON_CONTEXT_STARTED, ContextStartHandler2.class)
            .set(ContextConfiguration.ON_CONTEXT_STOP, ContextStopHandler2.class)
            .set(ContextConfiguration.IDENTIFIER, CONTEXT_2_IDENTIFIER)
            .build();

        activeContext.submitContext(contextConfiguration);

        synchronized (SubContextDriver.this) {
          SubContextDriver.this.state = State.CONTEXT_2_SUBMITTED;
        }


      } else if (activeContext.getId().equals(CONTEXT_2_IDENTIFIER)) {
        LOG.log(Level.INFO, "Received sub context. Closing");
        activeContext.close();
      }
    }
  }

  final class ContextClosedHandler implements EventHandler<ClosedContext> {

    @Override
    public void onNext(final ClosedContext closedContext) {

      LOG.log(Level.FINE, "Received ClosedContext: {0}", closedContext);

      if (closedContext.getId().equals(CONTEXT_2_IDENTIFIER)) {

        synchronized (SubContextDriver.this) {
          assert (SubContextDriver.this.state == State.CONTEXT_2_SUBMITTED);
        }

        closedContext.getParentContext().close();
        SubContextDriver.this.state = State.CONTEXT_2_CLOSED;

      } else if (closedContext.getId().equals(CONTEXT_1_IDENTIFIER)) {

        synchronized (SubContextDriver.this) {
          assert (SubContextDriver.this.state == State.CONTEXT_2_CLOSED);
        }

        throw new IllegalStateException("Received a closed context for the root context");
      }
    }
  }
}

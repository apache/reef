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
package com.microsoft.reef.examples.helloCLR;

import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.evaluator.EvaluatorType;
import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import com.microsoft.tang.proto.ClassHierarchyProto;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the Hello REEF Application
 */
@Unit
public final class HelloDriver {

  private static final Logger LOG = Logger.getLogger(HelloDriver.class.getName());

  private final EvaluatorRequestor requestor;

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  public HelloDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: ", startTime);
      HelloDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1).setSize(EvaluatorRequest.Size.SMALL).build());
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit an empty context and the HelloActivity
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting HelloCLR activity to AllocatedEvaluator: {0}", allocatedEvaluator);

      // Choose to run this evaluator on the CLR
      allocatedEvaluator.setType(EvaluatorType.CLR);
      try {
        final Configuration contextConfiguration = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "HelloREEFContext")
            .build();

        final ConfigurationBuilder activityConfigurationBuilder = Tang.Factory.getTang()
            .newConfigurationBuilder(loadClassHierarchy());
        activityConfigurationBuilder.bind("com.microsoft.reef.driver.activity.ActivityConfigurationOptions.Identifier", "Hello_From_CLR");
        activityConfigurationBuilder.bind("com.microsoft.reef.activity.IActivity", "com.microsoft.reef.activity.HelloActivity");
        allocatedEvaluator.submitContextAndActivity(contextConfiguration, activityConfigurationBuilder.build());
      } catch (final BindException ex) {
        final String message = "Unable to setup Activity or Context configuration.";
        LOG.log(Level.SEVERE, message, ex);
        throw new RuntimeException(message, ex);
      }
    }
  }


  private static ClassHierarchy loadClassHierarchy() {
    try (final InputStream chin = new FileInputStream(HelloCLR.CLASS_HIERARCHY_FILENAME)) {
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin); // A
      final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
      return ch;
    } catch (final IOException e) {
      final String message = "Unable to load class hierarchy.";
      LOG.log(Level.SEVERE, message, e);
      throw new RuntimeException(message, e);
    }
  }
}


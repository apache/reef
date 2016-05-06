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
package org.apache.reef.examples.group.broadcast;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.examples.group.bgd.operatornames.ControlMessageBroadcaster;
import org.apache.reef.examples.group.bgd.parameters.AllCommunicationGroup;
import org.apache.reef.examples.group.bgd.parameters.ModelDimensions;
import org.apache.reef.examples.group.broadcast.parameters.ModelBroadcaster;
import org.apache.reef.examples.group.broadcast.parameters.ModelReceiveAckReducer;
import org.apache.reef.examples.group.broadcast.parameters.NumberOfReceivers;
import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.api.driver.GroupCommDriver;
import org.apache.reef.io.network.group.impl.config.BroadcastOperatorSpec;
import org.apache.reef.io.network.group.impl.config.ReduceOperatorSpec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.poison.PoisonedConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver for broadcast example.
 */
@DriverSide
@Unit
public class BroadcastDriver {

  private static final Logger LOG = Logger.getLogger(BroadcastDriver.class.getName());

  private final AtomicBoolean masterSubmitted = new AtomicBoolean(false);
  private final AtomicInteger slaveIds = new AtomicInteger(0);
  private final AtomicInteger failureSet = new AtomicInteger(0);

  private final GroupCommDriver groupCommDriver;
  private final CommunicationGroupDriver allCommGroup;
  private final ConfigurationSerializer confSerializer;
  private final int dimensions;
  private final EvaluatorRequestor requestor;
  private final int numberOfReceivers;
  private final AtomicInteger numberOfAllocatedEvaluators;

  private String groupCommConfiguredMasterId;

  @Inject
  public BroadcastDriver(
      final EvaluatorRequestor requestor,
      final GroupCommDriver groupCommDriver,
      final ConfigurationSerializer confSerializer,
      @Parameter(ModelDimensions.class) final int dimensions,
      @Parameter(NumberOfReceivers.class) final int numberOfReceivers) {

    this.requestor = requestor;
    this.groupCommDriver = groupCommDriver;
    this.confSerializer = confSerializer;
    this.dimensions = dimensions;
    this.numberOfReceivers = numberOfReceivers;
    this.numberOfAllocatedEvaluators = new AtomicInteger(numberOfReceivers + 1);

    this.allCommGroup = this.groupCommDriver.newCommunicationGroup(
        AllCommunicationGroup.class, numberOfReceivers + 1);

    LOG.info("Obtained all communication group");

    this.allCommGroup
        .addBroadcast(ControlMessageBroadcaster.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(MasterTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addBroadcast(ModelBroadcaster.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(MasterTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addReduce(ModelReceiveAckReducer.class,
            ReduceOperatorSpec.newBuilder()
                .setReceiverId(MasterTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .setReduceFunctionClass(ModelReceiveAckReduceFunction.class)
                .build())
        .finalise();

    LOG.info("Added operators to allCommGroup");
  }

  /**
   * Handles the StartTime event: Request numOfReceivers Evaluators.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final int numEvals = BroadcastDriver.this.numberOfReceivers + 1;
      LOG.log(Level.FINE, "Requesting {0} evaluators", numEvals);
      BroadcastDriver.this.requestor.newRequest()
          .setNumber(numEvals)
          .setMemory(2048)
          .submit();
    }
  }

  /**
   * Handles AllocatedEvaluator: Submits a context with an id.
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting an id context to AllocatedEvaluator: {0}", allocatedEvaluator);
      final Configuration contextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "BroadcastContext-" +
              BroadcastDriver.this.numberOfAllocatedEvaluators.getAndDecrement())
          .build();
      allocatedEvaluator.submitContext(contextConfiguration);
    }
  }

  /**
   * FailedTask handler.
   */
  public class FailedTaskHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {

      LOG.log(Level.FINE, "Got failed Task: {0}", failedTask.getId());

      final ActiveContext activeContext = failedTask.getActiveContext().get();
      final Configuration partialTaskConf = Tang.Factory.getTang()
          .newConfigurationBuilder(
              TaskConfiguration.CONF
                  .set(TaskConfiguration.IDENTIFIER, failedTask.getId())
                  .set(TaskConfiguration.TASK, SlaveTask.class)
                  .build(),
              PoisonedConfiguration.TASK_CONF
                  .set(PoisonedConfiguration.CRASH_PROBABILITY, "0")
                  .set(PoisonedConfiguration.CRASH_TIMEOUT, "1")
                  .build())
          .bindNamedParameter(ModelDimensions.class, "" + dimensions)
          .build();

      // Do not add the task back:
      // allCommGroup.addTask(partialTaskConf);

      final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
      LOG.log(Level.FINER, "Submit SlaveTask conf: {0}", confSerializer.toString(taskConf));

      activeContext.submitTask(taskConf);
    }
  }

  /**
   * ActiveContext handler.
   */
  public class ContextActiveHandler implements EventHandler<ActiveContext> {

    private final AtomicBoolean storeMasterId = new AtomicBoolean(false);

    @Override
    public void onNext(final ActiveContext activeContext) {

      LOG.log(Level.FINE, "Got active context: {0}", activeContext.getId());

      /**
       * The active context can be either from data loading service or after network
       * service has loaded contexts. So check if the GroupCommDriver knows if it was
       * configured by one of the communication groups.
       */
      if (groupCommDriver.isConfigured(activeContext)) {

        if (activeContext.getId().equals(groupCommConfiguredMasterId) && !masterTaskSubmitted()) {

          final Configuration partialTaskConf = Tang.Factory.getTang()
              .newConfigurationBuilder(
                  TaskConfiguration.CONF
                      .set(TaskConfiguration.IDENTIFIER, MasterTask.TASK_ID)
                      .set(TaskConfiguration.TASK, MasterTask.class)
                      .build())
              .bindNamedParameter(ModelDimensions.class, Integer.toString(dimensions))
              .build();

          allCommGroup.addTask(partialTaskConf);

          final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
          LOG.log(Level.FINER, "Submit MasterTask conf: {0}", confSerializer.toString(taskConf));

          activeContext.submitTask(taskConf);

        } else {

          final Configuration partialTaskConf = Tang.Factory.getTang()
              .newConfigurationBuilder(
                  TaskConfiguration.CONF
                      .set(TaskConfiguration.IDENTIFIER, getSlaveId(activeContext))
                      .set(TaskConfiguration.TASK, SlaveTask.class)
                      .build(),
                  PoisonedConfiguration.TASK_CONF
                      .set(PoisonedConfiguration.CRASH_PROBABILITY, "0.4")
                      .set(PoisonedConfiguration.CRASH_TIMEOUT, "1")
                      .build())
              .bindNamedParameter(ModelDimensions.class, Integer.toString(dimensions))
              .build();

          allCommGroup.addTask(partialTaskConf);

          final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
          LOG.log(Level.FINER, "Submit SlaveTask conf: {0}", confSerializer.toString(taskConf));

          activeContext.submitTask(taskConf);
        }
      } else {

        final Configuration contextConf = groupCommDriver.getContextConfiguration();
        final String contextId = contextId(contextConf);

        if (storeMasterId.compareAndSet(false, true)) {
          groupCommConfiguredMasterId = contextId;
        }

        final Configuration serviceConf = groupCommDriver.getServiceConfiguration();
        LOG.log(Level.FINER, "Submit GCContext conf: {0}", confSerializer.toString(contextConf));
        LOG.log(Level.FINER, "Submit Service conf: {0}", confSerializer.toString(serviceConf));

        activeContext.submitContextAndService(contextConf, serviceConf);
      }
    }

    private String contextId(final Configuration contextConf) {
      try {
        final Injector injector = Tang.Factory.getTang().newInjector(contextConf);
        return injector.getNamedInstance(ContextIdentifier.class);
      } catch (final InjectionException e) {
        throw new RuntimeException("Unable to inject context identifier from context conf", e);
      }
    }

    private String getSlaveId(final ActiveContext activeContext) {
      return "SlaveTask-" + slaveIds.getAndIncrement();
    }

    private boolean masterTaskSubmitted() {
      return !masterSubmitted.compareAndSet(false, true);
    }
  }

  /**
   * ClosedContext handler.
   */
  public class ContextCloseHandler implements EventHandler<ClosedContext> {

    @Override
    public void onNext(final ClosedContext closedContext) {
      LOG.log(Level.FINE, "Got closed context: {0}", closedContext.getId());
      final ActiveContext parentContext = closedContext.getParentContext();
      if (parentContext != null) {
        LOG.log(Level.FINE, "Closing parent context: {0}", parentContext.getId());
        parentContext.close();
      }
    }
  }
}

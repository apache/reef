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
package org.apache.reef.examples.group.bgd;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.examples.group.bgd.data.parser.Parser;
import org.apache.reef.examples.group.bgd.data.parser.SVMLightParser;
import org.apache.reef.examples.group.bgd.loss.LossFunction;
import org.apache.reef.examples.group.bgd.operatornames.*;
import org.apache.reef.examples.group.bgd.parameters.AllCommunicationGroup;
import org.apache.reef.examples.group.bgd.parameters.BGDControlParameters;
import org.apache.reef.examples.group.bgd.parameters.ModelDimensions;
import org.apache.reef.examples.group.bgd.parameters.ProbabilityOfFailure;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.api.driver.GroupCommDriver;
import org.apache.reef.io.network.group.impl.config.BroadcastOperatorSpec;
import org.apache.reef.io.network.group.impl.config.ReduceOperatorSpec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.poison.PoisonedConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

@DriverSide
@Unit
public class BGDDriver {

  private static final Logger LOG = Logger.getLogger(BGDDriver.class.getName());

  private static final Tang TANG = Tang.Factory.getTang();

  private static final double STARTUP_FAILURE_PROB = 0.01;

  private final DataLoadingService dataLoadingService;
  private final GroupCommDriver groupCommDriver;
  private final ConfigurationSerializer confSerializer;
  private final CommunicationGroupDriver communicationsGroup;
  private final AtomicBoolean masterSubmitted = new AtomicBoolean(false);
  private final AtomicInteger slaveIds = new AtomicInteger(0);
  private final Map<String, RunningTask> runningTasks = new HashMap<>();
  private final AtomicBoolean jobComplete = new AtomicBoolean(false);
  private final Codec<ArrayList<Double>> lossCodec = new SerializableCodec<>();
  private final BGDControlParameters bgdControlParameters;

  private String communicationsGroupMasterContextId;

  @Inject
  public BGDDriver(final DataLoadingService dataLoadingService,
                   final GroupCommDriver groupCommDriver,
                   final ConfigurationSerializer confSerializer,
                   final BGDControlParameters bgdControlParameters) {
    this.dataLoadingService = dataLoadingService;
    this.groupCommDriver = groupCommDriver;
    this.confSerializer = confSerializer;
    this.bgdControlParameters = bgdControlParameters;

    final int minNumOfPartitions =
        bgdControlParameters.isRampup()
            ? bgdControlParameters.getMinParts()
            : dataLoadingService.getNumberOfPartitions();

    final int numParticipants = minNumOfPartitions + 1;

    this.communicationsGroup = this.groupCommDriver.newCommunicationGroup(
        AllCommunicationGroup.class, // NAME
        numParticipants);            // Number of participants

    LOG.log(Level.INFO,
        "Obtained entire communication group: start with {0} partitions", numParticipants);

    this.communicationsGroup
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
        .addReduce(LossAndGradientReducer.class,
            ReduceOperatorSpec.newBuilder()
                .setReceiverId(MasterTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .setReduceFunctionClass(LossAndGradientReduceFunction.class)
                .build())
        .addBroadcast(ModelAndDescentDirectionBroadcaster.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(MasterTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addBroadcast(DescentDirectionBroadcaster.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(MasterTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addReduce(LineSearchEvaluationsReducer.class,
            ReduceOperatorSpec.newBuilder()
                .setReceiverId(MasterTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .setReduceFunctionClass(LineSearchReduceFunction.class)
                .build())
        .addBroadcast(MinEtaBroadcaster.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(MasterTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .finalise();

    LOG.log(Level.INFO, "Added operators to communicationsGroup");
  }

  final class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {
      LOG.log(Level.INFO, "Got active context: {0}", activeContext.getId());
      if (jobRunning(activeContext)) {
        if (!groupCommDriver.isConfigured(activeContext)) {
          // The Context is not configured with the group communications service let's do that.
          submitGroupCommunicationsService(activeContext);
        } else {
          // The group communications service is already active on this context. We can submit the task.
          submitTask(activeContext);
        }
      }
    }

    /**
     * @param activeContext a context to be configured with group communications.
     */
    private void submitGroupCommunicationsService(final ActiveContext activeContext) {
      final Configuration contextConf = groupCommDriver.getContextConfiguration();
      final String contextId = getContextId(contextConf);
      final Configuration serviceConf;
      if (!dataLoadingService.isDataLoadedContext(activeContext)) {
        communicationsGroupMasterContextId = contextId;
        serviceConf = groupCommDriver.getServiceConfiguration();
      } else {
        final Configuration parsedDataServiceConf = ServiceConfiguration.CONF
            .set(ServiceConfiguration.SERVICES, ExampleList.class)
            .build();
        serviceConf = Tang.Factory.getTang()
            .newConfigurationBuilder(groupCommDriver.getServiceConfiguration(), parsedDataServiceConf)
            .bindImplementation(Parser.class, SVMLightParser.class)
            .build();
      }

      LOG.log(Level.FINEST, "Submit GCContext conf: {0} and Service conf: {1}", new Object[]{
          confSerializer.toString(contextConf), confSerializer.toString(serviceConf)});

      activeContext.submitContextAndService(contextConf, serviceConf);
    }

    private void submitTask(final ActiveContext activeContext) {

      assert (groupCommDriver.isConfigured(activeContext));

      final Configuration partialTaskConfiguration;
      if (activeContext.getId().equals(communicationsGroupMasterContextId) && !masterTaskSubmitted()) {
        partialTaskConfiguration = getMasterTaskConfiguration();
        LOG.info("Submitting MasterTask conf");
      } else {
        partialTaskConfiguration = getSlaveTaskConfiguration(getSlaveId(activeContext));
        // partialTaskConfiguration = Configurations.merge(
        //     getSlaveTaskConfiguration(getSlaveId(activeContext)),
        //     getTaskPoisonConfiguration());
        LOG.info("Submitting SlaveTask conf");
      }
      communicationsGroup.addTask(partialTaskConfiguration);
      final Configuration taskConfiguration = groupCommDriver.getTaskConfiguration(partialTaskConfiguration);
      LOG.log(Level.FINEST, "{0}", confSerializer.toString(taskConfiguration));
      activeContext.submitTask(taskConfiguration);
    }

    private boolean jobRunning(final ActiveContext activeContext) {
      synchronized (runningTasks) {
        if (!jobComplete.get()) {
          return true;
        } else {
          LOG.log(Level.INFO, "Job complete. Not submitting any task. Closing context {0}", activeContext);
          activeContext.close();
          return false;
        }
      }
    }
  }

  final class TaskRunningHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask runningTask) {
      synchronized (runningTasks) {
        if (!jobComplete.get()) {
          LOG.log(Level.INFO, "Job has not completed yet. Adding to runningTasks: {0}", runningTask);
          runningTasks.put(runningTask.getId(), runningTask);
        } else {
          LOG.log(Level.INFO, "Job complete. Closing context: {0}", runningTask.getActiveContext().getId());
          runningTask.getActiveContext().close();
        }
      }
    }
  }

  final class TaskFailedHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {

      final String failedTaskId = failedTask.getId();

      LOG.log(Level.WARNING, "Got failed Task: " + failedTaskId);

      if (jobRunning(failedTaskId)) {

        final ActiveContext activeContext = failedTask.getActiveContext().get();
        final Configuration partialTaskConf = getSlaveTaskConfiguration(failedTaskId);

        // Do not add the task back:
        // allCommGroup.addTask(partialTaskConf);

        final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
        LOG.log(Level.FINEST, "Submit SlaveTask conf: {0}", confSerializer.toString(taskConf));

        activeContext.submitTask(taskConf);
      }
    }

    private boolean jobRunning(final String failedTaskId) {
      synchronized (runningTasks) {
        if (!jobComplete.get()) {
          return true;
        } else {
          final RunningTask rTask = runningTasks.remove(failedTaskId);
          LOG.log(Level.INFO, "Job has completed. Not resubmitting");
          if (rTask != null) {
            LOG.log(Level.INFO, "Closing activecontext");
            rTask.getActiveContext().close();
          } else {
            LOG.log(Level.INFO, "Master must have closed my context");
          }
          return false;
        }
      }
    }
  }

  final class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask task) {
      LOG.log(Level.INFO, "Got CompletedTask: {0}", task.getId());
      final byte[] retVal = task.get();
      if (retVal != null) {
        final List<Double> losses = BGDDriver.this.lossCodec.decode(retVal);
        for (final Double loss : losses) {
          LOG.log(Level.INFO, "OUT: LOSS = {0}", loss);
        }
      }
      synchronized (runningTasks) {
        LOG.log(Level.INFO, "Acquired lock on runningTasks. Removing {0}", task.getId());
        final RunningTask rTask = runningTasks.remove(task.getId());
        if (rTask != null) {
          LOG.log(Level.INFO, "Closing active context: {0}", task.getActiveContext().getId());
          task.getActiveContext().close();
        } else {
          LOG.log(Level.INFO, "Master must have closed active context already for task {0}", task.getId());
        }

        if (MasterTask.TASK_ID.equals(task.getId())) {
          jobComplete.set(true);
          LOG.log(Level.INFO, "Master(=>Job) complete. Closing other running tasks: {0}", runningTasks.values());
          for (final RunningTask runTask : runningTasks.values()) {
            runTask.getActiveContext().close();
          }
          LOG.finest("Clearing runningTasks");
          runningTasks.clear();
        }
      }
    }
  }

  /**
   * @return Configuration for the MasterTask
   */
  public Configuration getMasterTaskConfiguration() {
    return Configurations.merge(
        TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, MasterTask.TASK_ID)
            .set(TaskConfiguration.TASK, MasterTask.class)
            .build(),
        bgdControlParameters.getConfiguration());
  }

  /**
   * @return Configuration for the SlaveTask
   */
  private Configuration getSlaveTaskConfiguration(final String taskId) {
    final double pSuccess = bgdControlParameters.getProbOfSuccessfulIteration();
    final int numberOfPartitions = dataLoadingService.getNumberOfPartitions();
    final double pFailure = 1 - Math.pow(pSuccess, 1.0 / numberOfPartitions);
    return Tang.Factory.getTang()
        .newConfigurationBuilder(
            TaskConfiguration.CONF
                .set(TaskConfiguration.IDENTIFIER, taskId)
                .set(TaskConfiguration.TASK, SlaveTask.class)
                .build())
        .bindNamedParameter(ModelDimensions.class, "" + bgdControlParameters.getDimensions())
        .bindImplementation(LossFunction.class, bgdControlParameters.getLossFunction())
        .bindNamedParameter(ProbabilityOfFailure.class, Double.toString(pFailure))
        .build();
  }

  private Configuration getTaskPoisonConfiguration() {
    return PoisonedConfiguration.TASK_CONF
        .set(PoisonedConfiguration.CRASH_PROBABILITY, STARTUP_FAILURE_PROB)
        .set(PoisonedConfiguration.CRASH_TIMEOUT, 1)
        .build();
  }

  private String getContextId(final Configuration contextConf) {
    try {
      return TANG.newInjector(contextConf).getNamedInstance(ContextIdentifier.class);
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

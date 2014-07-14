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
package com.microsoft.reef.examples.groupcomm.matmul;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.io.network.group.config.GroupOperators;
import com.microsoft.reef.io.network.impl.BindNSToTask;
import com.microsoft.reef.io.network.naming.NameServer;
import com.microsoft.reef.io.network.util.StringIdentifier;
import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.NetUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TaskSubmitter is responsible for submitting tasks to running evaluators.
 * <p/>
 * This is an event handler for events containing an iterable of running
 * evaluators. send, it creates the necessary structures to create the group
 * communication operator configurations and first submits the compute tasks.
 * <p/>
 * When all the compute tasks start, the driver will signal start of
 * controller through submitControlTask
 *
 * @author shravan
 */
public class TaskSubmitter implements EventHandler<Iterable<ActiveContext>> {

  /**
   * Standard Java logger object
   */
  private final Logger logger = Logger.getLogger(TaskSubmitter.class.getName());

  /**
   * The number of compute tasks
   */
  private final int numberOfComputeTasks;

  /**
   * The ids of the compute tasks
   */
  private final List<ComparableIdentifier> computeTaskIds;

  /**
   * The port numbers on which network service starts
   */
  private final List<Integer> nsPorts;

  private final StringIdentifierFactory factory = new StringIdentifierFactory();
  private final String nameServiceAddr;
  private final int nameServicePort;
  private final NameServer nameService;

  /**
   * Id of controller
   */
  private final ComparableIdentifier controllerId = (ComparableIdentifier) factory
      .getNewInstance("ControllerTask");

  /**
   * port of controller
   */
  private final int controllerPort = 7000;

  /**
   * The group communication operator configurations are managed through this
   */
  private GroupOperators operators;

  /**
   * Handle to the running evaluator that should run the controller
   */
  private ActiveContext controlerContext;

  /**
   * Constructor
   *
   * @param numberOfComputeTasks
   * @param nameServicePort
   */
  public TaskSubmitter(int numberOfComputeTasks, int nameServicePort) {
    this.numberOfComputeTasks = numberOfComputeTasks;
    computeTaskIds = new ArrayList<>(numberOfComputeTasks);
    nsPorts = new ArrayList<>(computeTaskIds.size());

    logger.log(Level.INFO,
        "Setting Up identifiers & ports for the network service to listen on");
    for (int i = 1; i <= numberOfComputeTasks; i++) {
      computeTaskIds.add((StringIdentifier) factory
          .getNewInstance("ComputeTask" + i));
      nsPorts.add(controllerPort + i);
    }

    // Starting Name Service
    nameServiceAddr = NetUtils.getLocalAddress();
    this.nameServicePort = nameServicePort;
    nameService = new NameServer(nameServicePort, factory);
  }

  /**
   * We have our list of {@link computeTasks} running Set up structures
   * required for group communication
   */
  @Override
  public void onNext(Iterable<ActiveContext> contexts) {
    logger.log(Level.INFO, "All context are running");
    logger.log(Level.INFO,
        "Setting Up Structures for creating Group Comm Operator Configurations");
    // TODO: After we fix issue #143, we need not worry about
    // setting up id to port mappings. We will let the service
    // choose a random port whose mapping will be made available
    // through the Name Service
    int runnEvalCnt = -1;
    List<ActiveContext> contextList = new ArrayList<>(numberOfComputeTasks);
    Map<ComparableIdentifier, Integer> id2port = new HashMap<>();
    for (ActiveContext context : contexts) {
      if (runnEvalCnt != -1) {
        contextList.add(context);
        // TODO: Review after #143
        final String hostAddr = context.getEvaluatorDescriptor().getNodeDescriptor()
            .getInetSocketAddress().getHostName();
//				String hostAddr = Utils.getLocalAddress();
        final int port = nsPorts.get(runnEvalCnt);
        final ComparableIdentifier compTaskId = computeTaskIds.get(runnEvalCnt);
        logger.log(Level.INFO, "Registering " + compTaskId + " with " + hostAddr + ":" + port);
        nameService.register(compTaskId, new InetSocketAddress(hostAddr,
            port));
        id2port.put(compTaskId, port);
      } else {
        controlerContext = context;
        // TODO: Review after #143
        final String hostAddr = context.getEvaluatorDescriptor().getNodeDescriptor()
            .getInetSocketAddress().getHostName();
//				String hostAddr = Utils.getLocalAddress();
        nameService.register(controllerId, new InetSocketAddress(
            hostAddr, controllerPort));
        id2port.put(controllerId, controllerPort);
      }
      ++runnEvalCnt;
    }

    logger.log(Level.INFO, "Creating Operator Configs");

    // All the group comm operators that we will use
    // will have VectorCodec and use VectorConcat as
    // the reduce function. The id2port should not be
    // there but we need to hold on for 143's fix
    operators = new GroupOperators(VectorCodec.class, VectorConcat.class,
        nameServiceAddr, nameServicePort, id2port);

    // Fluent syntax for adding the group communication
    // operators that are needed for the job
    operators.addScatter().setSender(controllerId)
        .setReceivers(computeTaskIds);

    operators.addBroadCast().setSender(controllerId)
        .setReceivers(computeTaskIds);

    // Each operator can override the default setting
    // Here the reduce function though same put it in
    // to illustrate
    operators.addReduce().setReceiver(controllerId)
        .setSenders(computeTaskIds).setRedFuncClass(VectorConcat.class);

    // Launch ComputeTasks first
    for (int i = 0; i < contextList.size(); i++) {
      final ComparableIdentifier compTaskId = computeTaskIds.get(i);
      contextList.get(i).submitTask(getComputeTaskConfig(compTaskId));
    }

    // All compute tasks have been launched
    // Wait for them to start running and
    // submitTask controller task later using controllerContext

  }

  /**
   * The {@link Configuration} for a {@link ComputeTask}
   * <p/>
   * Given the task id, the {@link GroupOperators} object will get you the
   * {@link Configuration} needed for Group Communication Operators on that
   * task
   *
   * @param compTaskId
   * @return
   */
  private Configuration getComputeTaskConfig(final ComparableIdentifier compTaskId) {
    try {
      // System.out.println(ConfigurationFile.toConfigurationString(operators.getConfig(compTaskId)));
      final JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
      b.addConfiguration(operators.getConfig(compTaskId));
      b.addConfiguration(TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, compTaskId.toString())
        .set(TaskConfiguration.TASK, ComputeTask.class)
        .set(TaskConfiguration.ON_TASK_STARTED, BindNSToTask.class)
        .build());
      return b.build();
    } catch (BindException e) {
      logger.log(
          Level.SEVERE,
          "BindException while creating GroupCommunication operator configurations",
          e.getCause());
      throw new RuntimeException(e);
    }
  }

  /**
   * Submits the {@link ControllerTask} using the {@link RunningEvaluator}
   * stored. We get the group communication configuration from
   * {@link GroupOperators} object
   */
  public void submitControlTask() {
    try {
      final JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
      b.addConfiguration(operators.getConfig(controllerId));
      b.addConfiguration(TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, controllerId.toString())
          .set(TaskConfiguration.TASK, ControllerTask.class)
          .set(TaskConfiguration.ON_TASK_STARTED, BindNSToTask.class)
          .build());
      controlerContext.submitTask(b.build());
    } catch (BindException e) {
      logger.log(
          Level.SEVERE,
          "BindException while creating GroupCommunication operator configurations",
          e.getCause());
      throw new RuntimeException(e);
    }
  }

  /**
   * Check if the id of the completed task matches that of the controller
   *
   * @param id
   * @return true if it matches false otherwise
   */
  public boolean controllerCompleted(String id) {
    return factory.getNewInstance(id).equals(controllerId);
  }

}

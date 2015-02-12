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
package org.apache.reef.examples.nggroup.matmul;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.examples.nggroup.utils.math.VectorCodec;
import org.apache.reef.io.network.group.config.GroupOperators;
import org.apache.reef.io.network.impl.BindNSToTask;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.NameServerImpl;
import org.apache.reef.io.network.util.StringIdentifier;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.ComparableIdentifier;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.NetUtils;

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
 */
public class TaskSubmitter implements EventHandler<Iterable<ActiveContext>> {

  /**
   * Standard Java LOG object
   */
  private static final Logger LOG = Logger.getLogger(TaskSubmitter.class.getName());

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
  private final ComparableIdentifier controllerId =
      (ComparableIdentifier) factory.getNewInstance("ControllerTask");

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

    LOG.log(Level.INFO,
        "Setting Up identifiers & ports for the network service to listen on");
    for (int i = 1; i <= numberOfComputeTasks; i++) {
      computeTaskIds.add((StringIdentifier) factory
          .getNewInstance("ComputeTask" + i));
      nsPorts.add(controllerPort + i);
    }

    // Starting Name Service
    nameServiceAddr = NetUtils.getLocalAddress();
    this.nameServicePort = nameServicePort;
    nameService = new NameServerImpl(nameServicePort, factory);
  }

  /**
   * We have our list of
   * {@link org.apache.reef.examples.nggroup.matmul.MatMultDriver.Parameters.ComputeTasks}
   * running. Set up structures required for group communication.
   */
  @Override
  public void onNext(Iterable<ActiveContext> contexts) {
    LOG.log(Level.INFO, "All context are running");
    LOG.log(Level.INFO,
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
        final int port = nsPorts.get(runnEvalCnt);
        final ComparableIdentifier compTaskId = computeTaskIds.get(runnEvalCnt);
        LOG.log(Level.INFO, "Registering " + compTaskId + " with " + hostAddr + ":" + port);
        nameService.register(compTaskId, new InetSocketAddress(hostAddr,
            port));
        id2port.put(compTaskId, port);
      } else {
        controlerContext = context;
        // TODO: Review after #143
        final String hostAddr = context.getEvaluatorDescriptor().getNodeDescriptor()
            .getInetSocketAddress().getHostName();
        nameService.register(controllerId, new InetSocketAddress(
            hostAddr, controllerPort));
        id2port.put(controllerId, controllerPort);
      }
      ++runnEvalCnt;
    }

    LOG.log(Level.INFO, "Creating Operator Configs");

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
   * task.
   */
  private Configuration getComputeTaskConfig(final ComparableIdentifier compTaskId) {

    final JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
    b.addConfiguration(operators.getConfig(compTaskId));
    b.addConfiguration(TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, compTaskId.toString())
        .set(TaskConfiguration.TASK, ComputeTask.class)
        .set(TaskConfiguration.ON_TASK_STARTED, BindNSToTask.class)
        .build());
    return b.build();
  }

  /**
   * Submits the {@link ControllerTask} using the
   * {@link org.apache.reef.driver.evaluator.AllocatedEvaluator}
   * stored. We get the group communication configuration from
   * {@link GroupOperators} object
   */
  public void submitControlTask() {
    final JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
    b.addConfiguration(operators.getConfig(controllerId));
    b.addConfiguration(TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, controllerId.toString())
        .set(TaskConfiguration.TASK, ControllerTask.class)
        .set(TaskConfiguration.ON_TASK_STARTED, BindNSToTask.class)
        .build());
    controlerContext.submitTask(b.build());
  }

  /**
   * Check if the id of the completed task matches that of the controller.
   * @return true if it matches false otherwise
   */
  public boolean controllerCompleted(String id) {
    return factory.getNewInstance(id).equals(controllerId);
  }
}

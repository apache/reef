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
package com.microsoft.reef.examples.groupcomm.matmul;

import com.microsoft.reef.driver.activity.ActivityConfiguration;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.io.network.group.config.GroupOperators;
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
 * Activity Submitter Is Responsible for submitting activities to running
 * evaluators
 * <p/>
 * This is an event handler for events containing an iterable of running
 * evaluators. onNext, it creates the necessary structures to create the group
 * communication operator configurations and first submits the compute
 * activities.
 * <p/>
 * When all the compute activities start, the driver will signal start of
 * controller through submitControlActivity
 *
 * @author shravan
 */
public class ActivitySubmitter implements EventHandler<Iterable<ActiveContext>> {
  /**
   * Standard Java logger object
   */
  private final Logger logger = Logger.getLogger(ActivitySubmitter.class
      .getName());

  /**
   * The number of compute activities
   */
  private final int numberOfComputeActivities;

  /**
   * The ids of the compute activities
   */
  private final List<ComparableIdentifier> computeActIds;

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
      .getNewInstance("ControllerActivity");

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
   * @param numberOfComputeActivities
   * @param nameServicePort
   */
  public ActivitySubmitter(int numberOfComputeActivities, int nameServicePort) {
    this.numberOfComputeActivities = numberOfComputeActivities;
    computeActIds = new ArrayList<>(numberOfComputeActivities);
    nsPorts = new ArrayList<>(computeActIds.size());

    logger.log(Level.INFO,
        "Setting Up identifiers & ports for the network service to listen on");
    for (int i = 1; i <= numberOfComputeActivities; i++) {
      computeActIds.add((StringIdentifier) factory
          .getNewInstance("ComputeActivity" + i));
      nsPorts.add(controllerPort + i);
    }

    // Starting Name Service
    nameServiceAddr = NetUtils.getLocalAddress();
    this.nameServicePort = nameServicePort;
    nameService = new NameServer(nameServicePort, factory);
  }

  /**
   * We have our list of {@link computeActivities} running Set up structures
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
    List<ActiveContext> contextList = new ArrayList<>(numberOfComputeActivities);
    Map<ComparableIdentifier, Integer> id2port = new HashMap<>();
    for (ActiveContext context : contexts) {
      if (runnEvalCnt != -1) {
        contextList.add(context);
        // TODO: Review after #143
        final String hostAddr = context.getEvaluatorDescriptor().getNodeDescriptor()
            .getInetSocketAddress().getHostName();
//				String hostAddr = Utils.getLocalAddress();
        final int port = nsPorts.get(runnEvalCnt);
        final ComparableIdentifier compActId = computeActIds.get(runnEvalCnt);
        logger.log(Level.INFO, "Registering " + compActId + " with " + hostAddr + ":" + port);
        nameService.register(compActId, new InetSocketAddress(hostAddr,
            port));
        id2port.put(compActId, port);
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
        .setReceivers(computeActIds);

    operators.addBroadCast().setSender(controllerId)
        .setReceivers(computeActIds);

    // Each operator can override the default setting
    // Here the reduce function though same put it in
    // to illustrate
    operators.addReduce().setReceiver(controllerId)
        .setSenders(computeActIds).setRedFuncClass(VectorConcat.class);

    // Launch ComputeActivities first
    for (int i = 0; i < contextList.size(); i++) {
      final ComparableIdentifier compActId = computeActIds.get(i);
      contextList.get(i).submitActivity(getComputeActivityConfig(compActId));
    }

    // All compute activities have been launched
    // Wait for them to start running and
    // submitActivity controller activity later using controlerContext

  }

  /**
   * The {@link Configuration} for a {@link ComputeActivity}
   * <p/>
   * Given the activity id, the {@link GroupOperators} object will get you the
   * {@link Configuration} needed for Group Communication Operators on that
   * activity
   *
   * @param compActId
   * @return
   */
  private Configuration getComputeActivityConfig(
      ComparableIdentifier compActId) {
    try {
//		  System.out.println(ConfigurationFile.toConfigurationString(operators.getConfig(compActId)));
      final JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
      b.addConfiguration(operators.getConfig(compActId));
      b.addConfiguration(ActivityConfiguration.CONF
          .set(ActivityConfiguration.IDENTIFIER, compActId.toString())
          .set(ActivityConfiguration.ACTIVITY, ComputeActivity.class)
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
   * Submits the {@link ControllerActivity} using the {@link RunningEvaluator}
   * stored. We get the group communication configuration from
   * {@link GroupOperators} object
   */
  public void submitControlActivity() {
    try {
      final JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
      b.addConfiguration(operators.getConfig(controllerId));
      b.addConfiguration(ActivityConfiguration.CONF
          .set(ActivityConfiguration.IDENTIFIER, controllerId.toString())
          .set(ActivityConfiguration.ACTIVITY, ControllerActivity.class)
          .build());
      controlerContext.submitActivity(b.build());
    } catch (BindException e) {
      logger.log(
          Level.SEVERE,
          "BindException while creating GroupCommunication operator configurations",
          e.getCause());
      throw new RuntimeException(e);
    }
  }

  /**
   * Check if the id of the completed activity matches that of the controller
   *
   * @param id
   * @return true if it matches false otherwise
   */
  public boolean controllerCompleted(String id) {
    return factory.getNewInstance(id).equals(controllerId);
  }

}

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
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.parameters.DriverIdentifier;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.OperatorSpec;
import com.microsoft.reef.io.network.nggroup.api.Topology;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.CommunicationGroupName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.OperatorName;
import com.microsoft.reef.io.network.nggroup.impl.config.parameters.SerializedOperConfigs;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EStage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 *
 */
public class CommunicationGroupDriverImpl implements CommunicationGroupDriver {

  private static final Logger LOG = Logger
      .getLogger(CommunicationGroupDriverImpl.class.getName());


  private final Class<? extends Name<String>> groupName;
  private final ConcurrentMap<Class<? extends Name<String>>, OperatorSpec> operatorSpecs = new ConcurrentHashMap<>();
  private final ConcurrentMap<Class<? extends Name<String>>, Topology> topologies = new ConcurrentHashMap<>();
  private final Map<String, TaskState> perTaskState = new HashMap<>();
  private boolean finalised = false;
  private final ConfigurationSerializer confSerializer;
  private final EStage<GroupCommMessage> senderStage;
  private final String driverId;
  private final int numberOfTasks;

  private final CountingSemaphore allTasksAdded;

  private final Object topologiesLock = new Object();
  private final Object configLock = new Object();
  private final AtomicBoolean initializing = new AtomicBoolean(true);


  private final Object yetToRunLock = new Object();
  private final Object toBeRemovedLock = new Object();

  private final SetMap<MsgKey, IndexedMsg> msgQue = new SetMap<>();

  public CommunicationGroupDriverImpl(final Class<? extends Name<String>> groupName,
                                      final ConfigurationSerializer confSerializer,
                                      final EStage<GroupCommMessage> senderStage,
                                      final BroadcastingEventHandler<RunningTask> commGroupRunningTaskHandler,
                                      final BroadcastingEventHandler<FailedTask> commGroupFailedTaskHandler,
                                      final BroadcastingEventHandler<FailedEvaluator> commGroupFailedEvaluatorHandler,
                                      final BroadcastingEventHandler<GroupCommMessage> commGroupMessageHandler,
                                      final String driverId,
                                      final int numberOfTasks) {
    super();
    this.groupName = groupName;
    this.numberOfTasks = numberOfTasks;
    this.driverId = driverId;
    this.confSerializer = confSerializer;
    this.senderStage = senderStage;
    this.allTasksAdded = new CountingSemaphore(numberOfTasks, getQualifiedName(), topologiesLock);

    final TopologyRunningTaskHandler topologyRunningTaskHandler = new TopologyRunningTaskHandler(this);
    commGroupRunningTaskHandler.addHandler(topologyRunningTaskHandler);
    final TopologyFailedTaskHandler topologyFailedTaskHandler = new TopologyFailedTaskHandler(this);
    commGroupFailedTaskHandler.addHandler(topologyFailedTaskHandler);
    final TopologyFailedEvaluatorHandler topologyFailedEvaluatorHandler = new TopologyFailedEvaluatorHandler(this);
    commGroupFailedEvaluatorHandler.addHandler(topologyFailedEvaluatorHandler);
    final TopologyMessageHandler topologyMessageHandler = new TopologyMessageHandler(this);
    commGroupMessageHandler.addHandler(topologyMessageHandler);
  }

  @Override
  public CommunicationGroupDriver addBroadcast(
      final Class<? extends Name<String>> operatorName, final BroadcastOperatorSpec spec) {
    if (finalised) {
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    }
    operatorSpecs.put(operatorName, spec);
    final Topology topology = new FlatTopology(senderStage, groupName, operatorName, driverId, numberOfTasks);
    topology.setRoot(spec.getSenderId());
    topology.setOperSpec(spec);
    topologies.put(operatorName, topology);

    return this;
  }

  @Override
  public CommunicationGroupDriver addReduce(
      final Class<? extends Name<String>> operatorName, final ReduceOperatorSpec spec) {
    if (finalised) {
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    }
    operatorSpecs.put(operatorName, spec);
    final Topology topology = new FlatTopology(senderStage, groupName, operatorName, driverId, numberOfTasks);
    topology.setRoot(spec.getReceiverId());
    topology.setOperSpec(spec);
    topologies.put(operatorName, topology);

    return this;
  }

  @Override
  public Configuration getConfiguration(final Configuration taskConf) {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final String taskId = taskId(taskConf);
    if (perTaskState.containsKey(taskId)) {
      jcb.bindNamedParameter(DriverIdentifier.class, driverId);
      jcb.bindNamedParameter(CommunicationGroupName.class, groupName.getName());
      synchronized (configLock) {
        LOG.info(getQualifiedName() + "Acquired configLock");
        while (cantGetConfig(taskId)) {
          LOG.info(getQualifiedName() + "Need to wait for failure");
          try {
            configLock.wait();
          } catch (final InterruptedException e) {
            throw new RuntimeException("InterruptedException while waiting on configLock", e);
          }
        }
        LOG.info(getQualifiedName() + taskId + " - Will fetch configuration now.");
      }
      LOG.info(getQualifiedName() + "Released configLock");
      synchronized (topologiesLock) {
        LOG.info(getQualifiedName() + "Acquired topologiesLock");
        for (final Map.Entry<Class<? extends Name<String>>, OperatorSpec> operSpecEntry : operatorSpecs
            .entrySet()) {
          final Class<? extends Name<String>> operName = operSpecEntry.getKey();
          final Topology topology = topologies.get(operName);
          final JavaConfigurationBuilder jcbInner = Tang.Factory.getTang()
              .newConfigurationBuilder(topology.getConfig(taskId));
          jcbInner.bindNamedParameter(DriverIdentifier.class, driverId);
          jcbInner.bindNamedParameter(OperatorName.class, operName.getName());
          jcb.bindSetEntry(SerializedOperConfigs.class,
              confSerializer.toString(jcbInner.build()));
        }
      }
      LOG.info(getQualifiedName() + "Released topologiesLock");
    }
    return jcb.build();
  }

  private boolean cantGetConfig(final String taskId) {
    LOG.info(getQualifiedName() + "Checking if I can't get config");
    final TaskState taskState = perTaskState.get(taskId);
    if (!taskState.equals(TaskState.NOT_STARTED)) {
      LOG.info(getQualifiedName() + taskId + " has started.");
      if (taskState.equals(TaskState.RUNNING)) {
        LOG.info(getQualifiedName() + "But " + taskId + " is running. We can't get config");
        return true;
      } else {
        LOG.info(getQualifiedName() + "But " + taskId + " has failed. We can get config");
        return false;
      }
    } else {
      LOG.info(getQualifiedName() + taskId + " has not started. We can get config");
      return false;
    }
  }

  @Override
  public void finalise() {
    finalised = true;
  }

  @Override
  public void addTask(final Configuration partialTaskConf) {
    final String taskId = taskId(partialTaskConf);
    synchronized (toBeRemovedLock) {
      LOG.info(getQualifiedName() + "Acquired toBeRemovedLock");
      while (perTaskState.containsKey(taskId)) {
        LOG.info(getQualifiedName() + "Trying to add an existing task. Will wait for removeTask");
        try {
          toBeRemovedLock.wait();
        } catch (final InterruptedException e) {
          throw new RuntimeException("InterruptedException", e);
        }
      }
    }
    LOG.info(getQualifiedName() + "Released toBeRemovedLock");
    synchronized (topologiesLock) {
      LOG.info(getQualifiedName() + "Acquired topologiesLock");
      for (final Class<? extends Name<String>> operName : operatorSpecs.keySet()) {
        final Topology topology = topologies.get(operName);
        topology.addTask(taskId);
      }
      perTaskState.put(taskId, TaskState.NOT_STARTED);
    }
    LOG.info(getQualifiedName() + "Released topologiesLock");
  }

  /**
   * @param id
   */
  public void removeTask(final String taskId) {
    LOG.warning(getQualifiedName() + "Removing Task as the evaluator has failed");
    synchronized (topologiesLock) {
      LOG.info(getQualifiedName() + "Acquired topologiesLock");
      for (final Class<? extends Name<String>> operName : operatorSpecs.keySet()) {
        final Topology topology = topologies.get(operName);
        topology.removeTask(taskId);
      }
      perTaskState.remove(taskId);
    }
    LOG.info(getQualifiedName() + "Released topologiesLock");
    LOG.info(getQualifiedName() + "Removed Task " + taskId + " Notifying waiting threads");
    synchronized (toBeRemovedLock) {
      LOG.info(getQualifiedName() + "Acquired toBeRemovedLock");
      toBeRemovedLock.notifyAll();
    }
    LOG.info(getQualifiedName() + "Released toBeRemovedLock");
  }


  /**
   * @param id
   */
  public void runTask(final String id) {
    LOG.info(getQualifiedName() + "Task-" + id + " running");

    synchronized (topologiesLock) {
      LOG.info(getQualifiedName() + "Acquired topologiesLock");
      for (final Class<? extends Name<String>> operName : operatorSpecs
          .keySet()) {
        final Topology topology = topologies.get(operName);
        topology.setRunning(id);
      }
      allTasksAdded.decrement();
      perTaskState.put(id, TaskState.RUNNING);
    }
    LOG.info(getQualifiedName() + "Released topologiesLock");
    synchronized (yetToRunLock) {
      LOG.info(getQualifiedName() + "Acquired yetToRunLock");
      yetToRunLock.notifyAll();
    }
    LOG.info(getQualifiedName() + "Released yetToRunLock");
  }

  /**
   * @param id
   */
  public void failTask(final String id) {
    LOG.info(getQualifiedName() + "Task-" + id + " failed");
    synchronized (yetToRunLock) {
      LOG.info(getQualifiedName() + "Acquired yetToRunLock");
      while (cantFailTask(id)) {
        LOG.info(getQualifiedName() + "Need to wait for it run");
        try {
          yetToRunLock.wait();
        } catch (final InterruptedException e) {
          throw new RuntimeException("InterruptedException while waiting on yetToRunLock", e);
        }
      }
      LOG.info(getQualifiedName() + id + " - Can safely set failure.");
    }
    LOG.info(getQualifiedName() + "Released yetToRunLock");
    synchronized (topologiesLock) {
      LOG.info(getQualifiedName() + "Acquired topologiesLock");
      for (final Class<? extends Name<String>> operName : operatorSpecs
          .keySet()) {
        final Topology topology = topologies.get(operName);
        topology.setFailed(id);
      }
      allTasksAdded.increment();
      perTaskState.put(id, TaskState.FAILED);
      LOG.info(getQualifiedName() + "Removing msgs associated with dead task "
          + id + " from msgQue.");
      final Set<MsgKey> keys = msgQue.keySet();
      final List<MsgKey> keysToBeRemoved = new ArrayList<>();
      for (final MsgKey msgKey : keys) {
        if (msgKey.getSrc().equals(id)) {
          keysToBeRemoved.add(msgKey);
        }
      }
      LOG.info(getQualifiedName() + keysToBeRemoved + " keys that will be removed");
      for (final MsgKey key : keysToBeRemoved) {
        msgQue.remove(key);
      }
    }
    LOG.info(getQualifiedName() + "Released topologiesLock");
    synchronized (configLock) {
      LOG.info(getQualifiedName() + "Acquired configLock");
      configLock.notifyAll();
    }
    LOG.info(getQualifiedName() + "Released configLock");
  }


  /**
   * @param id
   * @return
   */
  private boolean cantFailTask(final String taskId) {
    LOG.info(getQualifiedName() + "Checking if I can't fail task");
    final TaskState taskState = perTaskState.get(taskId);
    if (!taskState.equals(TaskState.NOT_STARTED)) {
      LOG.info(getQualifiedName() + taskId + " has started.");
      if (!taskState.equals(TaskState.RUNNING)) {
        LOG.info(getQualifiedName() + "But " + taskId + " is not running yet. Can't set failure");
        return true;
      } else {
        LOG.info(getQualifiedName() + "But " + taskId + " is running. Can set failure");
        return false;
      }
    } else {
      final String msg = getQualifiedName() + taskId + " has not started. We can't fail a task that hasn't started";
      LOG.info(msg);
      return true;
    }
  }

  public void queNProcessMsg(final GroupCommMessage msg) {
    LOG.info(getQualifiedName() + "Queing and processing " + msg.getType()
        + " from " + msg.getSrcid());
    final IndexedMsg indMsg = new IndexedMsg(msg);
    final Class<? extends Name<String>> operName = indMsg.getOperName();
    final MsgKey key = new MsgKey(msg);
    if (msgQue.contains(key, indMsg)) {
      final String mesg = getQualifiedName() + "MsgQue already contains " + msg.getType() +
          " msg for " + key + " in " + Utils.simpleName(operName);
      LOG.warning(mesg);
      throw new RuntimeException(mesg);
    }
    LOG.info(getQualifiedName() + "Adding msg to que");
    msgQue.add(key, indMsg);
    if (msgQue.count(key) == topologies.size()) {
      LOG.info(getQualifiedName() + "MsgQue for " + key + " contains " + msg.getType()
          + " msgs from: " + msgQue.get(key));
      for (final IndexedMsg innerIndMsg : msgQue.remove(key)) {
        topologies.get(innerIndMsg.getOperName()).processMsg(innerIndMsg.getMsg());
      }
      LOG.info(getQualifiedName() + "All msgs processed and removed");
    }
  }

  private boolean isMsgVersionOk(final GroupCommMessage msg) {
    if (!msg.hasVersion()) {
      throw new RuntimeException(getQualifiedName()
          + "can only deal with versioned msgs");
    }
    final String srcId = msg.getSrcid();
    final int rcvVersion = msg.getSrcVersion();

    final int version = topologies.get(Utils.getClass(msg.getOperatorname()))
        .getNodeVersion(srcId);
    if (rcvVersion < version) {
      LOG.warning(getQualifiedName() + "received a ver-" + rcvVersion
          + " msg while expecting ver-" + version + ". Discarding msg");
      return false;
    }
    if (rcvVersion > version) {
      LOG.warning(getQualifiedName() + "received a HIGHER ver-" + rcvVersion
          + " msg while expecting ver-" + version + ". Something fishy!!!");
      return false;
    }
    return true;
  }

  /**
   * @param msg
   */
  public void processMsg(final GroupCommMessage msg) {
    LOG.info(getQualifiedName() + "processing " + msg.getType() + " from "
        + msg.getSrcid());
    if (!isMsgVersionOk(msg)) {
      return;
    }
    synchronized (topologiesLock) {
      LOG.info(getQualifiedName() + "Acquired topologiesLock");
      if (initializing.get() || msg.getType().equals(Type.UpdateTopology)) {
        LOG.info(getQualifiedName() + "waiting for all nodes to run");
        allTasksAdded.await();
        initializing.compareAndSet(true, false);
      }
      queNProcessMsg(msg);
    }
    LOG.info(getQualifiedName() + "Released topologiesLock");
  }

  private String taskId(final Configuration partialTaskConf) {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(partialTaskConf);
      return injector.getNamedInstance(TaskConfigurationOptions.Identifier.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to find task identifier", e);
    }
  }

  /**
   * @return
   */
  private String getQualifiedName() {
    return Utils.simpleName(groupName) + " - ";
  }
}

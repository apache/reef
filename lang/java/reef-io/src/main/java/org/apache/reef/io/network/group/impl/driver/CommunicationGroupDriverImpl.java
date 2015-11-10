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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.network.group.api.config.OperatorSpec;
import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.api.driver.Topology;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.config.BroadcastOperatorSpec;
import org.apache.reef.io.network.group.impl.config.GatherOperatorSpec;
import org.apache.reef.io.network.group.impl.config.ReduceOperatorSpec;
import org.apache.reef.io.network.group.impl.config.ScatterOperatorSpec;
import org.apache.reef.io.network.group.impl.config.parameters.*;
import org.apache.reef.io.network.group.impl.utils.BroadcastingEventHandler;
import org.apache.reef.io.network.group.impl.utils.CountingSemaphore;
import org.apache.reef.io.network.group.impl.utils.SetMap;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.impl.SingleThreadStage;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

@DriverSide
@Private
public class CommunicationGroupDriverImpl implements CommunicationGroupDriver {

  private static final Logger LOG = Logger.getLogger(CommunicationGroupDriverImpl.class.getName());

  private final Class<? extends Name<String>> groupName;
  private final ConcurrentMap<Class<? extends Name<String>>, OperatorSpec> operatorSpecs = new ConcurrentHashMap<>();
  private final ConcurrentMap<Class<? extends Name<String>>, Topology> topologies = new ConcurrentHashMap<>();
  private final Map<String, TaskState> perTaskState = new HashMap<>();
  private boolean finalised = false;
  private final ConfigurationSerializer confSerializer;
  private final String driverId;

  private final CountingSemaphore allInitialTasksRunning;

  private final Object topologiesLock = new Object();
  private final Object configLock = new Object();
  private final AtomicBoolean initializing = new AtomicBoolean(true);

  private final Object yetToRunLock = new Object();
  private final Object toBeRemovedLock = new Object();

  private final SetMap<MsgKey, IndexedMsg> msgQue = new SetMap<>();

  private final TopologyFactory topologyFactory;
  private final Class<? extends Topology> topologyClass;

  /**
   * @deprecated in 0.14. Use Tang to obtain an instance of this instead.
   */
  @Deprecated
  public CommunicationGroupDriverImpl(final Class<? extends Name<String>> groupName,
                                      final ConfigurationSerializer confSerializer,
                                      final EStage<GroupCommunicationMessage> senderStage,
                                      final BroadcastingEventHandler<RunningTask> groupCommRunningTaskHandler,
                                      final BroadcastingEventHandler<FailedTask> groupCommFailedTaskHandler,
                                      final BroadcastingEventHandler<FailedEvaluator> groupCommFailedEvaluatorHandler,
                                      final BroadcastingEventHandler<GroupCommunicationMessage> commGroupMessageHandler,
                                      final String driverId, final int numberOfTasks, final int fanOut) {
    super();
    this.groupName = groupName;
    this.driverId = driverId;
    this.confSerializer = confSerializer;
    this.allInitialTasksRunning = new CountingSemaphore(numberOfTasks, getQualifiedName(), topologiesLock);

    groupCommRunningTaskHandler.addHandler(new TopologyRunningTaskHandler(this));
    groupCommFailedTaskHandler.addHandler(new TopologyFailedTaskHandler(this));
    groupCommFailedEvaluatorHandler.addHandler(new TopologyFailedEvaluatorHandler(this));
    commGroupMessageHandler.addHandler(new TopologyMessageHandler(this));
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(CommGroupNameClass.class, groupName);
    injector.bindVolatileParameter(GroupCommSenderStage.class, senderStage);
    injector.bindVolatileParameter(DriverIdentifier.class, driverId);
    injector.bindVolatileParameter(CommGroupNumTask.class, numberOfTasks);
    injector.bindVolatileParameter(TreeTopologyFanOut.class, fanOut);
    try {
      topologyFactory = injector.getInstance(TopologyFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
    this.topologyClass = TreeTopology.class;
  }

  @Inject
  private CommunicationGroupDriverImpl(
      @Parameter(CommGroupNameClass.class) final Class<? extends Name<String>> groupName,
      final ConfigurationSerializer confSerializer,
      @Parameter(GroupCommRunningTaskHandler.class)
          final BroadcastingEventHandler<RunningTask> groupCommRunningTaskHandler,
      @Parameter(GroupCommFailedTaskHandler.class)
          final BroadcastingEventHandler<FailedTask> groupCommFailedTaskHandler,
      @Parameter(GroupCommFailedEvalHandler.class)
          final BroadcastingEventHandler<FailedEvaluator> groupCommFailedEvaluatorHandler,
          final GroupCommMessageHandler groupCommMessageHandler,
      @Parameter(DriverIdentifier.class) final String driverId,
      @Parameter(CommGroupNumTask.class) final int numberOfTasks,
      final TopologyFactory topologyFactory,
      @Parameter(TopologyClass.class) final Class<? extends Topology> topologyClass) {
    super();
    this.groupName = groupName;
    this.driverId = driverId;
    this.confSerializer = confSerializer;
    this.allInitialTasksRunning = new CountingSemaphore(numberOfTasks, getQualifiedName(), topologiesLock);

    registerHandlers(groupCommRunningTaskHandler, groupCommFailedTaskHandler,
        groupCommFailedEvaluatorHandler, groupCommMessageHandler);
    this.topologyFactory = topologyFactory;
    this.topologyClass = topologyClass;
  }

  private void registerHandlers(
      final BroadcastingEventHandler<RunningTask> runningTaskHandler,
      final BroadcastingEventHandler<FailedTask> failedTaskHandler,
      final BroadcastingEventHandler<FailedEvaluator> failedEvaluatorHandler,
      final GroupCommMessageHandler groupCommMessageHandler) {
    runningTaskHandler.addHandler(new TopologyRunningTaskHandler(this));
    failedTaskHandler.addHandler(new TopologyFailedTaskHandler(this));
    failedEvaluatorHandler.addHandler(new TopologyFailedEvaluatorHandler(this));
    groupCommMessageHandler.addHandler(groupName, new SingleThreadStage<>(new TopologyMessageHandler(this), 100 * 100));
  }

  @Override
  public CommunicationGroupDriver addBroadcast(final Class<? extends Name<String>> operatorName,
                                               final BroadcastOperatorSpec spec) {
    LOG.entering("CommunicationGroupDriverImpl", "addBroadcast",
        new Object[]{getQualifiedName(), Utils.simpleName(operatorName), spec});
    if (finalised) {
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    }
    operatorSpecs.put(operatorName, spec);

    final Topology topology;
    try {
      topology = topologyFactory.getNewInstance(operatorName, topologyClass);
    } catch (final InjectionException e) {
      LOG.log(Level.WARNING, "Cannot inject new topology named {0}", operatorName);
      throw new RuntimeException(e);
    }

    topology.setRootTask(spec.getSenderId());
    topology.setOperatorSpecification(spec);
    topologies.put(operatorName, topology);
    LOG.exiting("CommunicationGroupDriverImpl", "addBroadcast",
        Arrays.toString(new Object[]{getQualifiedName(), Utils.simpleName(operatorName), " added"}));
    return this;
  }

  @Override
  public CommunicationGroupDriver addReduce(final Class<? extends Name<String>> operatorName,
                                            final ReduceOperatorSpec spec) {
    LOG.entering("CommunicationGroupDriverImpl", "addReduce",
        new Object[]{getQualifiedName(), Utils.simpleName(operatorName), spec});
    if (finalised) {
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    }
    LOG.finer(getQualifiedName() + "Adding reduce operator to tree topology: " + spec);
    operatorSpecs.put(operatorName, spec);

    final Topology topology;
    try {
      topology = topologyFactory.getNewInstance(operatorName, topologyClass);
    } catch (final InjectionException e) {
      LOG.log(Level.WARNING, "Cannot inject new topology named {0}", operatorName);
      throw new RuntimeException(e);
    }

    topology.setRootTask(spec.getReceiverId());
    topology.setOperatorSpecification(spec);
    topologies.put(operatorName, topology);
    LOG.exiting("CommunicationGroupDriverImpl", "addReduce",
        Arrays.toString(new Object[]{getQualifiedName(), Utils.simpleName(operatorName), " added"}));
    return this;
  }

  @Override
  public CommunicationGroupDriver addScatter(final Class<? extends Name<String>> operatorName,
                                             final ScatterOperatorSpec spec) {
    LOG.entering("CommunicationGroupDriverImpl", "addScatter",
        new Object[]{getQualifiedName(), Utils.simpleName(operatorName), spec});
    if (finalised) {
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    }
    operatorSpecs.put(operatorName, spec);

    final Topology topology;
    try {
      topology = topologyFactory.getNewInstance(operatorName, topologyClass);
    } catch (final InjectionException e) {
      LOG.log(Level.WARNING, "Cannot inject new topology named {0}", operatorName);
      throw new RuntimeException(e);
    }

    topology.setRootTask(spec.getSenderId());
    topology.setOperatorSpecification(spec);
    topologies.put(operatorName, topology);
    LOG.exiting("CommunicationGroupDriverImpl", "addScatter",
        Arrays.toString(new Object[]{getQualifiedName(), Utils.simpleName(operatorName), spec}));
    return this;
  }

  @Override
  public CommunicationGroupDriver addGather(final Class<? extends Name<String>> operatorName,
                                            final GatherOperatorSpec spec) {
    LOG.entering("CommunicationGroupDriverImpl", "addGather",
        new Object[]{getQualifiedName(), Utils.simpleName(operatorName), spec});
    if (finalised) {
      throw new IllegalStateException("Can't add more operators to a finalised spec");
    }
    operatorSpecs.put(operatorName, spec);

    final Topology topology;
    try {
      topology = topologyFactory.getNewInstance(operatorName, topologyClass);
    } catch (final InjectionException e) {
      LOG.log(Level.WARNING, "Cannot inject new topology named {0}", operatorName);
      throw new RuntimeException(e);
    }

    topology.setRootTask(spec.getReceiverId());
    topology.setOperatorSpecification(spec);
    topologies.put(operatorName, topology);
    LOG.exiting("CommunicationGroupDriverImpl", "addGather",
        Arrays.toString(new Object[]{getQualifiedName(), Utils.simpleName(operatorName), spec}));
    return this;
  }

  @Override
  public Configuration getTaskConfiguration(final Configuration taskConf) {
    LOG.entering("CommunicationGroupDriverImpl", "getTaskConfiguration",
        new Object[]{getQualifiedName(), confSerializer.toString(taskConf)});
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final String taskId = taskId(taskConf);
    if (perTaskState.containsKey(taskId)) {
      jcb.bindNamedParameter(CommunicationGroupName.class, groupName.getName());
      LOG.finest(getQualifiedName() + "Task has been added. Waiting to acquire configLock");
      synchronized (configLock) {
        LOG.finest(getQualifiedName() + "Acquired configLock");
        while (cantGetConfig(taskId)) {
          LOG.finest(getQualifiedName() + "Need to wait for failure");
          try {
            configLock.wait();
          } catch (final InterruptedException e) {
            throw new RuntimeException(getQualifiedName() + "InterruptedException while waiting on configLock", e);
          }
        }
        LOG.finest(getQualifiedName() + taskId + " - Will fetch configuration now.");
        LOG.finest(getQualifiedName() + "Released configLock. Waiting to acquire topologiesLock");
      }
      synchronized (topologiesLock) {
        LOG.finest(getQualifiedName() + "Acquired topologiesLock");
        for (final Map.Entry<Class<? extends Name<String>>, OperatorSpec> operSpecEntry : operatorSpecs.entrySet()) {
          final Class<? extends Name<String>> operName = operSpecEntry.getKey();
          final Topology topology = topologies.get(operName);
          final JavaConfigurationBuilder jcbInner = Tang.Factory.getTang()
              .newConfigurationBuilder(topology.getTaskConfiguration(taskId));
          jcbInner.bindNamedParameter(DriverIdentifier.class, driverId);
          jcbInner.bindNamedParameter(OperatorName.class, operName.getName());
          jcb.bindSetEntry(SerializedOperConfigs.class, confSerializer.toString(jcbInner.build()));
        }
        LOG.finest(getQualifiedName() + "Released topologiesLock");
      }
    } else {
      return null;
    }
    final Configuration configuration = jcb.build();
    LOG.exiting("CommunicationGroupDriverImpl", "getTaskConfiguration",
        Arrays.toString(new Object[]{getQualifiedName(), confSerializer.toString(configuration)}));
    return configuration;
  }

  private boolean cantGetConfig(final String taskId) {
    LOG.entering("CommunicationGroupDriverImpl", "cantGetConfig", new Object[]{getQualifiedName(), taskId});
    final TaskState taskState = perTaskState.get(taskId);
    if (!taskState.equals(TaskState.NOT_STARTED)) {
      LOG.finest(getQualifiedName() + taskId + " has started.");
      if (taskState.equals(TaskState.RUNNING)) {
        LOG.exiting("CommunicationGroupDriverImpl", "cantGetConfig",
            Arrays.toString(new Object[]{true, getQualifiedName(), taskId, " is running. We can't get config"}));
        return true;
      } else {
        LOG.exiting("CommunicationGroupDriverImpl", "cantGetConfig",
            Arrays.toString(new Object[]{false, getQualifiedName(), taskId, " has failed. We can get config"}));
        return false;
      }
    } else {
      LOG.exiting("CommunicationGroupDriverImpl", "cantGetConfig",
          Arrays.toString(new Object[]{false, getQualifiedName(), taskId, " has not started. We can get config"}));
      return false;
    }
  }

  @Override
  public void finalise() {
    finalised = true;
  }

  @Override
  public void addTask(final Configuration partialTaskConf) {
    LOG.entering("CommunicationGroupDriverImpl", "addTask",
        new Object[]{getQualifiedName(), confSerializer.toString(partialTaskConf)});
    final String taskId = taskId(partialTaskConf);
    LOG.finest(getQualifiedName() + "AddTask(" + taskId + "). Waiting to acquire toBeRemovedLock");
    synchronized (toBeRemovedLock) {
      LOG.finest(getQualifiedName() + "Acquired toBeRemovedLock");
      while (perTaskState.containsKey(taskId)) {
        LOG.finest(getQualifiedName() + "Trying to add an existing task. Will wait for removeTask");
        try {
          toBeRemovedLock.wait();
        } catch (final InterruptedException e) {
          throw new RuntimeException(getQualifiedName() + "InterruptedException while waiting on toBeRemovedLock", e);
        }
      }
      LOG.finest(getQualifiedName() + "Released toBeRemovedLock. Waiting to acquire topologiesLock");
    }
    synchronized (topologiesLock) {
      LOG.finest(getQualifiedName() + "Acquired topologiesLock");

      boolean isRootOfSomeTopology = false;
      for (final Class<? extends Name<String>> operName : operatorSpecs.keySet()) {
        final Topology topology = topologies.get(operName);
        topology.addTask(taskId);
        isRootOfSomeTopology |= topology.getRootId().equals(taskId);
      }

      if (isRootOfSomeTopology) {
        topologiesLock.notifyAll();
      }

      perTaskState.put(taskId, TaskState.NOT_STARTED);
      LOG.finest(getQualifiedName() + "Released topologiesLock");
    }
    LOG.fine(getQualifiedName() + "Added " + taskId + " to topology");
    LOG.exiting("CommunicationGroupDriverImpl", "addTask",
        Arrays.toString(new Object[]{getQualifiedName(), "Added task: ", taskId}));
  }

  public void removeTask(final String taskId) {
    LOG.entering("CommunicationGroupDriverImpl", "removeTask", new Object[]{getQualifiedName(), taskId});
    LOG.info(getQualifiedName() + "Removing Task " + taskId +
        " as the evaluator has failed.");
    LOG.finest(getQualifiedName() + "Remove Task(" + taskId +
        "): Waiting to acquire topologiesLock");
    synchronized (topologiesLock) {
      LOG.finest(getQualifiedName() + "Acquired topologiesLock");
      for (final Class<? extends Name<String>> operName : operatorSpecs.keySet()) {
        final Topology topology = topologies.get(operName);
        topology.removeTask(taskId);
      }
      perTaskState.remove(taskId);
      LOG.finest(getQualifiedName() + "Released topologiesLock. Waiting to acquire toBeRemovedLock");
    }
    synchronized (toBeRemovedLock) {
      LOG.finest(getQualifiedName() + "Acquired toBeRemovedLock");
      LOG.finest(getQualifiedName() + "Removed Task " + taskId + " Notifying waiting threads");
      toBeRemovedLock.notifyAll();
      LOG.finest(getQualifiedName() + "Released toBeRemovedLock");
    }
    LOG.fine(getQualifiedName() + "Removed " + taskId + " to topology");
    LOG.exiting("CommunicationGroupDriverImpl", "removeTask",
        Arrays.toString(new Object[]{getQualifiedName(), "Removed task: ", taskId}));
  }

  public void runTask(final String id) {
    LOG.entering("CommunicationGroupDriverImpl", "runTask", new Object[]{getQualifiedName(), id});
    LOG.finest(getQualifiedName() + "Task-" + id + " running. Waiting to acquire topologiesLock");
    LOG.fine(getQualifiedName() + "Got running Task: " + id);

    boolean nonMember = false;
    synchronized (topologiesLock) {
      if (perTaskState.containsKey(id)) {
        LOG.finest(getQualifiedName() + "Acquired topologiesLock");

        for (final Class<? extends Name<String>> operName : operatorSpecs.keySet()) {
          final Topology topology = topologies.get(operName);
          while (!topology.isRootPresent() && !topology.getRootId().equals(id)) {
            try {
              // wait until the root node has been added to the topology
              topologiesLock.wait();
            } catch (final InterruptedException e) {
              throw new RuntimeException(getQualifiedName() +
                  "InterruptedException while waiting on topologiesLock", e);
            }
          }
        }

        // This loop shouldn't be merged with the one above, because the one above contains a lock.wait().
        // All topologies must be modified at one go, without giving up the turn.
        for (final Class<? extends Name<String>> operName : operatorSpecs.keySet()) {
          final Topology topology = topologies.get(operName);
          topology.onRunningTask(id);
        }
        if (initializing.get()) {
          allInitialTasksRunning.decrement();
        }
        perTaskState.put(id, TaskState.RUNNING);
        LOG.finest(getQualifiedName() + "Released topologiesLock. Waiting to acquire yetToRunLock");
      } else {
        nonMember = true;
      }
    }
    synchronized (yetToRunLock) {
      LOG.finest(getQualifiedName() + "Acquired yetToRunLock");
      yetToRunLock.notifyAll();
      LOG.finest(getQualifiedName() + "Released yetToRunLock");
    }
    if (nonMember) {
      LOG.exiting("CommunicationGroupDriverImpl", "runTask",
          getQualifiedName() + id + " does not belong to this communication group. Ignoring");
    } else {
      LOG.fine(getQualifiedName() + "Status of task " + id + " changed to RUNNING");
      LOG.exiting("CommunicationGroupDriverImpl", "runTask",
          Arrays.toString(new Object[]{getQualifiedName(), "Set running complete on task ", id}));
    }
  }

  public void failTask(final String id) {
    LOG.entering("CommunicationGroupDriverImpl", "failTask", new Object[]{getQualifiedName(), id});
    LOG.finest(getQualifiedName() + "Task-" + id + " failed. Waiting to acquire yetToRunLock");
    LOG.fine(getQualifiedName() + "Got failed Task: " + id);
    synchronized (yetToRunLock) {
      LOG.finest(getQualifiedName() + "Acquired yetToRunLock");
      // maybe the task does not belong to this communication group.
      // if it doesn't, we return, it should belong to other group
      // which will handle its failure
      if (!perTaskState.containsKey(id)) {
        LOG.fine(getQualifiedName()
            + " does not have this task, another communicationGroup must have it");
        return;
      }
      while (cantFailTask(id)) {
        LOG.finest(getQualifiedName() + "Need to wait for it run");
        try {
          yetToRunLock.wait();
        } catch (final InterruptedException e) {
          throw new RuntimeException(getQualifiedName() + "InterruptedException while waiting on yetToRunLock", e);
        }
      }
      LOG.finest(getQualifiedName() + id + " - Can safely set failure.");
      LOG.finest(getQualifiedName() + "Released yetToRunLock. Waiting to acquire topologiesLock");
    }
    synchronized (topologiesLock) {
      LOG.finest(getQualifiedName() + "Acquired topologiesLock");
      for (final Class<? extends Name<String>> operName : operatorSpecs.keySet()) {
        final Topology topology = topologies.get(operName);
        topology.onFailedTask(id);
      }
      if (initializing.get()) {
        allInitialTasksRunning.increment();
      }
      perTaskState.put(id, TaskState.FAILED);
      LOG.finest(getQualifiedName() + "Removing msgs associated with dead task " + id + " from msgQue.");
      final Set<MsgKey> keys = msgQue.keySet();
      final List<MsgKey> keysToBeRemoved = new ArrayList<>();
      for (final MsgKey msgKey : keys) {
        if (msgKey.getSrc().equals(id)) {
          keysToBeRemoved.add(msgKey);
        }
      }
      LOG.finest(getQualifiedName() + keysToBeRemoved + " keys that will be removed");
      for (final MsgKey key : keysToBeRemoved) {
        msgQue.remove(key);
      }
      LOG.finest(getQualifiedName() + "Released topologiesLock. Waiting to acquire configLock");
    }
    synchronized (configLock) {
      LOG.finest(getQualifiedName() + "Acquired configLock");
      configLock.notifyAll();
      LOG.finest(getQualifiedName() + "Released configLock");
    }
    LOG.fine(getQualifiedName() + "Status of task " + id + " changed to FAILED");
    LOG.exiting("CommunicationGroupDriverImpl", "failTask",
        Arrays.toString(new Object[]{getQualifiedName(), "Set failed complete on task ", id}));
  }

  private boolean cantFailTask(final String taskId) {
    LOG.entering("CommunicationGroupDriverImpl", "cantFailTask", new Object[]{getQualifiedName(), taskId});
    final TaskState taskState = perTaskState.get(taskId);
    if (!taskState.equals(TaskState.NOT_STARTED)) {
      LOG.finest(getQualifiedName() + taskId + " has started.");
      if (!taskState.equals(TaskState.RUNNING)) {
        LOG.exiting("CommunicationGroupDriverImpl", "cantFailTask",
            Arrays.toString(new Object[]{true, getQualifiedName(), taskId, " is not running yet. Can't set failure"}));
        return true;
      } else {
        LOG.exiting("CommunicationGroupDriverImpl", "cantFailTask",
            Arrays.toString(new Object[]{false, getQualifiedName(), taskId, " is running. Can set failure"}));
        return false;
      }
    } else {
      LOG.exiting("CommunicationGroupDriverImpl", "cantFailTask",
          Arrays.toString(new Object[]{true, getQualifiedName(), taskId,
              " has not started. We can't fail a task that hasn't started"}));
      return true;
    }
  }

  public void queNProcessMsg(final GroupCommunicationMessage msg) {
    LOG.entering("CommunicationGroupDriverImpl", "queNProcessMsg", new Object[]{getQualifiedName(), msg});
    final IndexedMsg indMsg = new IndexedMsg(msg);
    final Class<? extends Name<String>> operName = indMsg.getOperName();
    final MsgKey key = new MsgKey(msg);
    if (msgQue.contains(key, indMsg)) {
      throw new RuntimeException(getQualifiedName() + "MsgQue already contains " + msg.getType() + " msg for " + key +
          " in " + Utils.simpleName(operName));
    }
    LOG.finest(getQualifiedName() + "Adding msg to que");
    msgQue.add(key, indMsg);
    if (msgQue.count(key) == topologies.size()) {
      LOG.finest(getQualifiedName() + "MsgQue for " + key + " contains " + msg.getType() + " msgs from: "
          + msgQue.get(key));
      for (final IndexedMsg innerIndMsg : msgQue.remove(key)) {
        topologies.get(innerIndMsg.getOperName()).onReceiptOfMessage(innerIndMsg.getMsg());
      }
      LOG.finest(getQualifiedName() + "All msgs processed and removed");
    }
    LOG.exiting("CommunicationGroupDriverImpl", "queNProcessMsg",
        Arrays.toString(new Object[]{getQualifiedName(), "Que & Process done for: ", msg}));
  }

  private boolean isMsgVersionOk(final GroupCommunicationMessage msg) {
    LOG.entering("CommunicationGroupDriverImpl", "isMsgVersionOk", new Object[]{getQualifiedName(), msg});
    if (msg.hasVersion()) {
      final String srcId = msg.getSrcid();
      final int rcvSrcVersion = msg.getSrcVersion();
      final int expSrcVersion = topologies.get(Utils.getClass(msg.getOperatorname())).getNodeVersion(srcId);

      final boolean srcVersionChk = chkVersion(rcvSrcVersion, expSrcVersion, "Src Version Check: ");
      LOG.exiting("CommunicationGroupDriverImpl", "isMsgVersionOk",
          Arrays.toString(new Object[]{srcVersionChk, getQualifiedName(), msg}));
      return srcVersionChk;
    } else {
      throw new RuntimeException(getQualifiedName() + "can only deal with versioned msgs");
    }
  }

  private boolean chkVersion(final int rcvVersion, final int version, final String msg) {
    if (rcvVersion < version) {
      LOG.warning(getQualifiedName() + msg + "received a ver-" + rcvVersion + " msg while expecting ver-" + version);
      return false;
    }
    if (rcvVersion > version) {
      LOG.warning(getQualifiedName() + msg + "received a HIGHER ver-" + rcvVersion + " msg while expecting ver-"
          + version + ". Something fishy!!!");
      return false;
    }
    return true;
  }

  public void processMsg(final GroupCommunicationMessage msg) {
    LOG.entering("CommunicationGroupDriverImpl", "processMsg", new Object[]{getQualifiedName(), msg});
    LOG.finest(getQualifiedName() + "ProcessMsg: " + msg + ". Waiting to acquire topologiesLock");
    synchronized (topologiesLock) {
      LOG.finest(getQualifiedName() + "Acquired topologiesLock");
      if (!isMsgVersionOk(msg)) {
        LOG.finer(getQualifiedName() + "Discarding msg. Released topologiesLock");
        return;
      }
      if (initializing.get()) {
        LOG.fine(getQualifiedName() + msg.getSimpleOperName() + ": Waiting for all required(" +
            allInitialTasksRunning.getInitialCount() + ") nodes to run");
        allInitialTasksRunning.await();
        LOG.fine(getQualifiedName() + msg.getSimpleOperName() + ": All required(" +
            allInitialTasksRunning.getInitialCount() + ") nodes are running");
        initializing.compareAndSet(true, false);
      }
      queNProcessMsg(msg);
      LOG.finest(getQualifiedName() + "Released topologiesLock");
    }
    LOG.exiting("CommunicationGroupDriverImpl", "processMsg",
        Arrays.toString(new Object[]{getQualifiedName(), "ProcessMsg done for: ", msg}));
  }

  private String taskId(final Configuration partialTaskConf) {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(partialTaskConf);
      return injector.getNamedInstance(TaskConfigurationOptions.Identifier.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(getQualifiedName() +
          "Injection exception while extracting taskId from partialTaskConf", e);
    }
  }

  private String getQualifiedName() {
    return Utils.simpleName(groupName) + " - ";
  }
}

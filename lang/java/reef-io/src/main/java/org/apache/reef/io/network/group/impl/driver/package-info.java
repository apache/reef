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

/**
 * This package contains the implementation of the driver side of the
 * Group Communication Service using the tree/flat topology. The Service
 * can be configured with many named Communication Groups and each
 * Communication Group can be configured with many named operators. This
 * configuration is typically done on the GroupCommDriver object injected
 * into the driver. During this specification only the root nodes are
 * specified.
 *
 * After the GroupCommDriver is configured the driver would want to submit
 * tasks with the Communication Groups and their operators configured. To do
 * that the user has to create a partial task configuration containing his
 * set of configurations and add it to the relevant communication group.
 * Based on whether the given task is a Master/Slave different roles for
 * the operators are configured. Once added, the final Configuration containing
 * operators and their roles encoded can be obtained by the
 * CommunicationGroupDriver.getConfiguration() call. The topology is complete
 * once the minimum number of tasks needed for the group to function have been
 * added.
 *
 * The initial configuration dished out by the service
 * creates a bunch of tasks that are not connected. Connections are
 * established when the Tasks start running. Each operator defines its own
 * topology and can have potentially different root nodes. Each node in the
 * topology called a TaskNode is a logical representation of a running Task.
 * Adding a task to a group creates its TaskNode with TaskState NOT_STARTED.
 *
 * The driver side of the service plays a major role in setting up the
 * topology and making sure that topology is set-up only when the parties
 * involved are ready to participate in the communication. A topology will
 * not contain parties that are not active. Active status is given to parties
 * who have acknowledged the presence of their neighbors and who have been
 * acknowledged by their neighbors as present. The connection between two
 * parties is initiated by the driver and the driver then expects the parties
 * to ACK that their end of the connection has been set-up. Once a party has
 * ACK its end of all connections and all its neighbors ACK the outgoing part
 * of their connection to this party the driver sends a TopologySetup msg to
 * indicate that the topology is usable by this party now. The driver also
 * listens in on failure events and appropriately updates its state so that
 * it does not wait for ACKs from failed tasks.
 *
 * There are two chains of control:
 * 1. Driver Events (Running/Failed Tasks)
 * 2. Tasks (Msgs sent by Task side of Group Communication Service)
 *
 * All events and msgs are funneled through the CommunicationGroupDriver so
 * that all the topologies belonging to different operators configured on the
 * group are in sync. Without this there is possibility of a deadlock between
 * the tasks and the driver. So there is no finer level locking other than that
 * in the CommunicationGroupDriver.
 *
 * 1. Driver Events
 *  These are routed to all communication groups and each communication group
 *  routes it to all topologies. The topology will then route this event to the
 *  corresponding TaskNode which will process that event. When a task starts
 *  running it is notified of its running neighbors and the running neighbors
 *  are notified of its running. The TaskNodeStatus object keeps track of the
 *  state of the local topology for this TaskNode. What msgs have been sent to
 *  this node that need to be ACKed, the status of its neighbors and whether
 *  this TaskNode is ready to accept data from a neighboring TaskNode when we
 *  ask the neighbor to check if he was only waiting for this TaskNode to ACK
 *  in order to send TopologySetup. So when we are sending (Parent|Child)(Add|Dead)
 *  msgs we first note that we expect an ACK back for this. These ACK expectations
 *  are then deleted if the node fails. Neighbor failures are also updated.
 *  All the msg sending is done by the TaskNode. The TaskNodeStatus is only a
 *  state manager to consult on the status of ACKs and neighbors. This is needed
 *  by the chkAndSendTopSetup logic. These events also send msgs related to failure
 *  of tasks so that any task in the toplogy that waited for a response from the
 *  failed task can move on.
 *
 * 2. Tasks
 *  We get ACK msgs from tasks and they update the status of ACK expectations.
 *  Here the TaskNodeStatus acts as a bridge between the initiation of a link
 *  between two parties and the final set-up of the link. Once all ACKs have
 *  been received we ask the TaskNode to check if it is ready to send a
 *  TopologySetup msg. Every ACK can also trigger the chkAndSendTopSetup for
 *  a neighbor.
 *
 * The above concerns the topology set-up and fault notiifcations. However, the
 * other major task that the driver helps with is in updating a topology. When
 * requested for the update of a Topology using the UpdateTopology msg, the
 * driver notifies all the parties that have to update their topologies by
 * sending an UpdateTopology msg to the affected parties. The tasks then try to
 * enter the UpdateToplogy phase and as soon as they can do(by acquiring a lock)
 * they respond that they have done so. The driver will wait till all the affected
 * parties do so and then sends the initiator a msg that Topology has been updated.
 * The effect of this is that the unaffected regions of the topology can continue
 * to work normally while the affected regions are healing. The affected regions
 * heal their (local)topologies using the TopologySetup mechanism described above.
 * Both update topology and inital set-up use the minimum number of tasks to define
 * when the set-up is complete.
 *
 * The CommGroupDriver class also takes care of minor alterations to event ordering
 * by the use of locks to coerce the events to occur in a definite way.
 *
 */
package org.apache.reef.io.network.group.impl.driver;

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
package com.microsoft.reef.io.network.group.impl;

import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.group.impl.operators.basic.config.GroupParameters;
import com.microsoft.reef.io.network.group.operators.Gather;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Handler to be registered with {@link NetworkService} to cater to
 * {@link Message}s  of type {@link GroupCommMessage}
 * <p/>
 * Uses a {@link Handler} per operators to receive from each registered task
 * Essentially a {@link GroupCommMessage} sent from a task using an operator will
 * end up with the {@link Handler} for that operator. These messages are queued in
 * a {@link BlockingQueue} and receive calls made by a receiving operator for a message
 * from a task will block until the message from that task arrives in the queue
 * <p/>
 * The queue capacity determines how many messages from the same task can be enqueued
 */
public class GroupCommNetworkHandler implements EventHandler<Message<GroupCommMessage>> {

  /**
   * Inner per operator {@link Handler}
   */
  private static class GCMHandler implements Handler {
    /**
     * One queue per registered id
     */
    Map<Identifier, BlockingQueue<GroupCommMessage>> idQues;

    IdentifierFactory idFactory;

    /**
     * Queue capacity - num messages held from the same task and operator
     */
    int capacity;

    /**
     * Constructor for fields.
     * Also initializes one empty queue per registered id
     *
     * @param ids
     * @param factory
     * @param capacity
     */
    public GCMHandler(List<Identifier> ids, IdentifierFactory factory, int capacity) {
      idQues = new HashMap<Identifier, BlockingQueue<GroupCommMessage>>(ids.size());
      idFactory = factory;
      this.capacity = capacity;
      for (Identifier id : ids) {
        idQues.put(id, new LinkedBlockingQueue<GroupCommMessage>(this.capacity));
      }
    }

    /**
     * Block till data is available in the queue from this source
     * TODO: This actually blocks on a particular source. Also support
     * polling for operations like {@link Gather} which do not care
     */
    @Override
    public GroupCommMessage getData(Identifier srcId) throws InterruptedException {
      return idQues.get(srcId).take();
    }

    /**
     * Message received from the sender of operator this handler services
     * Add it to the queue representing the source
     */
    @Override
    public void onNext(GroupCommMessage data) {
      Identifier srcId = idFactory.getNewInstance(data.getSrcid());
      idQues.get(srcId).add(data);
    }
  }

  /**
   * Per operator {@link Handler}
   */
  Map<GroupCommMessage.Type, GCMHandler> handlerMap;

  @NamedParameter(doc = "List of Identifiers on which the handler should listen")
  public static class IDs implements Name<String> {
    //intentionally blank
  }

  @NamedParameter(doc = "Queue Capacity for the handler of each operation type", default_value = "5")
  public static class QueueCapacity implements Name<Integer> {
    //intentionally blank
  }

  /**
   * Register the ids that this handler instance should service
   *
   * @param ids
   * @param factory
   * @param capacity
   */
  @Inject
  public GroupCommNetworkHandler(
      @Parameter(IDs.class) String ids,
      @Parameter(GroupParameters.IDFactory.class) IdentifierFactory factory,
      @Parameter(QueueCapacity.class) int capacity) {
    this(Utils.parseList(ids, factory), factory, capacity);
  }

  /**
   * Constructor without @Parameters
   * <p/>
   * Add one {@link Handler} per operator supported
   * for the ids registered
   *
   * @param ids
   * @param factory
   * @param capacity
   */
  public GroupCommNetworkHandler(List<Identifier> ids, IdentifierFactory factory, int capacity) {
    handlerMap = new HashMap<>();
    handlerMap.put(Type.AllGather, new GCMHandler(ids, factory, capacity));
    handlerMap.put(Type.AllReduce, new GCMHandler(ids, factory, capacity));
    handlerMap.put(Type.Broadcast, new GCMHandler(ids, factory, capacity));
    handlerMap.put(Type.Gather, new GCMHandler(ids, factory, capacity));
    handlerMap.put(Type.Reduce, new GCMHandler(ids, factory, capacity));
    handlerMap.put(Type.ReduceScatter, new GCMHandler(ids, factory, capacity));
    handlerMap.put(Type.Scatter, new GCMHandler(ids, factory, capacity));
  }

  /**
   * Get {@link Handler} for the supplied operator
   *
   * @param type
   * @return {@link Handler} on which we can wait for the message to be sent by a source
   */
  public Handler getHandler(GroupCommMessage.Type type) {
    return handlerMap.get(type);
  }

  /**
   * When a message arrives
   * Check which operator sent it and
   * call send on the {@link Handler} for that operator
   * <p/>
   * which will enque this message under the respective source
   */
  @Override
  public void onNext(Message<GroupCommMessage> msg) {
    GroupCommMessage data = msg.getData().iterator().next();
    getHandler(data.getType()).onNext(data);
  }
}

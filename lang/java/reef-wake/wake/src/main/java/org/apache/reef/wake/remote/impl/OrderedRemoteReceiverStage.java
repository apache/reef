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
package org.apache.reef.wake.remote.impl;

import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.WakeParameters;
import org.apache.reef.wake.impl.DefaultThreadFactory;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receive incoming events and dispatch to correct handlers in order.
 */
public class OrderedRemoteReceiverStage implements EStage<TransportEvent> {

  private static final Logger LOG = Logger.getLogger(OrderedRemoteReceiverStage.class.getName());

  private static final String CLASS_NAME = OrderedRemoteReceiverStage.class.getSimpleName();

  private static final long SHUTDOWN_TIMEOUT = WakeParameters.REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT;

  private final ExecutorService pushExecutor;
  private final ExecutorService pullExecutor;

  private final ThreadPoolStage<TransportEvent> pushStage;

  /**
   * Constructs an ordered remote receiver stage.
   *
   * @param handler      the handler of remote events
   * @param errorHandler the exception handler
   */
  public OrderedRemoteReceiverStage(
      final EventHandler<RemoteEvent<byte[]>> handler, final EventHandler<Throwable> errorHandler) {

    this.pushExecutor = Executors.newCachedThreadPool(new DefaultThreadFactory(CLASS_NAME + ":Push"));
    this.pullExecutor = Executors.newCachedThreadPool(new DefaultThreadFactory(CLASS_NAME + ":Pull"));

    final ConcurrentMap<SocketAddress, OrderedEventStream> streamMap = new ConcurrentHashMap<>();

    final ThreadPoolStage<OrderedEventStream> pullStage = new ThreadPoolStage<>(
        new OrderedPullEventHandler(handler), this.pullExecutor, errorHandler);

    this.pushStage = new ThreadPoolStage<>(
        new OrderedPushEventHandler(streamMap, pullStage), this.pushExecutor, errorHandler); // for decoupling
  }

  @Override
  public void onNext(final TransportEvent value) {
    LOG.log(Level.FINEST, "Push: {0}", value);
    this.pushStage.onNext(value);
  }

  @Override
  public void close() throws Exception {
    close("PushExecutor", this.pushExecutor);
    close("PullExecutor", this.pullExecutor);
  }

  private static void close(final String name, final ExecutorService executor) {
    LOG.log(Level.FINE, "Close {0} begin", name);
    executor.shutdown();
    try {
      // wait for threads to finish for timeout
      if (!executor.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "{0}: Executor did not terminate in {1} ms.", new Object[] {name, SHUTDOWN_TIMEOUT});
        final List<Runnable> droppedRunnables = executor.shutdownNow();
        LOG.log(Level.WARNING, "{0}: Executor dropped {1} tasks.", new Object[] {name, droppedRunnables.size()});
      }
    } catch (final InterruptedException e) {
      LOG.log(Level.WARNING, "Close interrupted");
      throw new RemoteRuntimeException(e);
    }
    LOG.log(Level.FINE, "Close {0} end", name);
  }
}

class OrderedPushEventHandler implements EventHandler<TransportEvent> {

  private static final Logger LOG = Logger.getLogger(OrderedPushEventHandler.class.getName());

  private final RemoteEventCodec<byte[]> codec;
  private final ConcurrentMap<SocketAddress, OrderedEventStream> streamMap; // per remote address
  private final ThreadPoolStage<OrderedEventStream> pullStage;

  OrderedPushEventHandler(final ConcurrentMap<SocketAddress, OrderedEventStream> streamMap,
                          final ThreadPoolStage<OrderedEventStream> pullStage) {
    this.codec = new RemoteEventCodec<>(new ByteCodec());
    this.streamMap = streamMap;
    this.pullStage = pullStage;
  }

  @Override
  public void onNext(final TransportEvent value) {
    final RemoteEvent<byte[]> re = codec.decode(value.getData());
    re.setLocalAddress(value.getLocalAddress());
    re.setRemoteAddress(value.getRemoteAddress());

    if (LOG.isLoggable(Level.FINER)) {
      LOG.log(Level.FINER, "{0} {1}", new Object[]{value, re});
    }

    LOG.log(Level.FINER, "Value length is {0}", value.getData().length);

    final SocketAddress addr = re.remoteAddress();
    OrderedEventStream stream = streamMap.get(re.remoteAddress());
    if (stream == null) {
      stream = new OrderedEventStream();
      if (streamMap.putIfAbsent(addr, stream) != null) {
        stream = streamMap.get(addr);
      }
    }
    stream.add(re);
    pullStage.onNext(stream);
  }
}

class OrderedPullEventHandler implements EventHandler<OrderedEventStream> {

  private static final Logger LOG = Logger.getLogger(OrderedPullEventHandler.class.getName());

  private final EventHandler<RemoteEvent<byte[]>> handler;

  OrderedPullEventHandler(final EventHandler<RemoteEvent<byte[]>> handler) {
    this.handler = handler;
  }

  @Override
  public void onNext(final OrderedEventStream stream) {
    if (LOG.isLoggable(Level.FINER)) {
      LOG.log(Level.FINER, "{0}", stream);
    }

    synchronized (stream) {
      RemoteEvent<byte[]> event;
      while ((event = stream.consume()) != null) {
        handler.onNext(event);
      }
    }
  }
}

class OrderedEventStream {
  private static final Logger LOG = Logger.getLogger(OrderedEventStream.class.getName());
  private final BlockingQueue<RemoteEvent<byte[]>> queue; // a queue of remote events
  private long nextSeq; // the number of the next event to consume

  OrderedEventStream() {
    queue = new PriorityBlockingQueue<>(11, new RemoteEventComparator<byte[]>());
    nextSeq = 0;
  }

  synchronized void add(final RemoteEvent<byte[]> event) {
    queue.add(event);
  }

  synchronized RemoteEvent<byte[]> consume() {
    RemoteEvent<byte[]> event = queue.peek();
    if (event != null) {

      if (event.getSeq() == nextSeq) {
        event = queue.poll();
        ++nextSeq;
        return event;
      } else {
        LOG.log(Level.FINER, "Event sequence is {0} does not match expected {1}",
            new Object[]{event.getSeq(), nextSeq});
      }
    } else {
      LOG.log(Level.FINER, "Event is null");
    }

    return null;
  }
}

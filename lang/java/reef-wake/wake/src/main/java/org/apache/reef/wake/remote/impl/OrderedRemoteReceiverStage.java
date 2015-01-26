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
 * Receive incoming events and dispatch to correct handlers in order
 */
public class OrderedRemoteReceiverStage implements EStage<TransportEvent> {

  private static final Logger LOG = Logger.getLogger(OrderedRemoteReceiverStage.class.getName());
  private final long shutdownTimeout = WakeParameters.REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT;

  private final ConcurrentMap<SocketAddress, OrderedEventStream> streamMap;
  private final ExecutorService pushExecutor;
  private final ExecutorService pullExecutor;

  private final ThreadPoolStage<TransportEvent> pushStage;
  private final ThreadPoolStage<OrderedEventStream> pullStage;

  /**
   * Constructs a ordered remote receiver stage
   *
   * @param handler      the handler of remote events
   * @param errorHandler the exception handler
   */
  public OrderedRemoteReceiverStage(EventHandler<RemoteEvent<byte[]>> handler, EventHandler<Throwable> errorHandler) {
    this.streamMap = new ConcurrentHashMap<SocketAddress, OrderedEventStream>();

    this.pushExecutor = Executors.newCachedThreadPool(new DefaultThreadFactory(OrderedRemoteReceiverStage.class.getName() + "_Push"));
    this.pullExecutor = Executors.newCachedThreadPool(new DefaultThreadFactory(OrderedRemoteReceiverStage.class.getName() + "_Pull"));

    this.pullStage = new ThreadPoolStage<OrderedEventStream>(new OrderedPullEventHandler(handler), this.pullExecutor, errorHandler);
    this.pushStage = new ThreadPoolStage<TransportEvent>(new OrderedPushEventHandler(streamMap, pullStage), this.pushExecutor, errorHandler); // for decoupling
  }

  @Override
  public void onNext(TransportEvent value) {
    LOG.log(Level.FINEST, "{0}", value);
    pushStage.onNext(value);
  }

  @Override
  public void close() throws Exception {
    LOG.log(Level.FINE, "close");

    if (pushExecutor != null) {
      pushExecutor.shutdown();
      try {
        // wait for threads to finish for timeout
        if (!pushExecutor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
          LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
          List<Runnable> droppedRunnables = pushExecutor.shutdownNow();
          LOG.log(Level.WARNING, "Executor dropped " + droppedRunnables.size() + " tasks.");
        }
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Close interrupted");
        throw new RemoteRuntimeException(e);
      }
    }

    if (pullExecutor != null) {
      pullExecutor.shutdown();
      try {
        // wait for threads to finish for timeout
        if (!pullExecutor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
          LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
          List<Runnable> droppedRunnables = pullExecutor.shutdownNow();
          LOG.log(Level.WARNING, "Executor dropped " + droppedRunnables.size() + " tasks.");
        }
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Close interrupted");
        throw new RemoteRuntimeException(e);
      }
    }
  }
}

class OrderedPushEventHandler implements EventHandler<TransportEvent> {

  private static final Logger LOG = Logger.getLogger(OrderedPushEventHandler.class.getName());

  private final RemoteEventCodec<byte[]> codec;
  private final ConcurrentMap<SocketAddress, OrderedEventStream> streamMap; // per remote address
  private final ThreadPoolStage<OrderedEventStream> pullStage;

  OrderedPushEventHandler(ConcurrentMap<SocketAddress, OrderedEventStream> streamMap,
                          ThreadPoolStage<OrderedEventStream> pullStage) {
    this.codec = new RemoteEventCodec<byte[]>(new ByteCodec());
    this.streamMap = streamMap;
    this.pullStage = pullStage;
  }

  @Override
  public void onNext(TransportEvent value) {
    RemoteEvent<byte[]> re = codec.decode(value.getData());
    re.setLocalAddress(value.getLocalAddress());
    re.setRemoteAddress(value.getRemoteAddress());

    if (LOG.isLoggable(Level.FINER))
      LOG.log(Level.FINER, "{0} {1}", new Object[]{value, re});

    LOG.log(Level.FINER, "Value length is {0}", value.getData().length);

    SocketAddress addr = re.remoteAddress();
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

  OrderedPullEventHandler(EventHandler<RemoteEvent<byte[]>> handler) {
    this.handler = handler;
  }

  @Override
  public void onNext(OrderedEventStream stream) {
    if (LOG.isLoggable(Level.FINER))
      LOG.log(Level.FINER, "{0}", stream);

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
    queue = new PriorityBlockingQueue<RemoteEvent<byte[]>>(11, new RemoteEventComparator<byte[]>());
    nextSeq = 0;
  }

  synchronized void add(RemoteEvent<byte[]> event) {
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
        LOG.log(Level.FINER, "Event sequence is {0} does not match expected {1}", new Object[]{event.getSeq(), nextSeq});
      }
    } else {
      LOG.log(Level.FINER, "Event is null");
    }

    return null;
  }
}

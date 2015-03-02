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
package org.apache.reef.io.network.impl;

import org.apache.reef.io.network.exception.NetworkRuntimeException;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.WakeParameters;
import org.apache.reef.wake.impl.DefaultThreadFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.LinkListener;
import org.apache.reef.wake.remote.transport.Transport;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stage sends events to established connections and if a connection has not been established yet,
 * the event will be queued to wait until the connection is complete and then be handled.
 *
 * The sending of the events is thread safe.
 */
final class NetworkSenderStage implements EStage<NetworkServiceEvent> {

  private static final long SHUTDOWN_TIMEOUT = WakeParameters.REMOTE_EXECUTOR_SHUTDOWN_TIMEOUT;

  private static final Logger LOG = Logger.getLogger(NetworkSenderStage.class.getName());

  private final ReadWriteLock readWriteLock;
  private final IdentifierFactory idFactory;
  private final Transport transport;
  private final Codec<NetworkServiceEvent> codec;
  private final NetworkPreconfiguredMap preconfiguredMap;
  private final LinkListener<NetworkServiceEvent> linkListener;
  private final ExecutorService executor;
  private final ConcurrentMap<SocketAddress, Link<NetworkServiceEvent>> linkMap;
  private final Map<SocketAddress, ExpirableConcurrentQueue<NetworkServiceEvent>> queueMap;
  private final int connectingQueueSizeThreshold;
  private final int connectingQueueWaitTime;

  NetworkSenderStage(
      final Transport transport,
      final Codec<NetworkServiceEvent> codec,
      final IdentifierFactory idFactory,
      final NetworkPreconfiguredMap preconfiguredMap,
      final int numThreads,
      final int connectingQueueSizeThreshold,
      final int connectingQueueWaitTime) {

    this.idFactory = idFactory;
    this.transport = transport;
    this.preconfiguredMap = preconfiguredMap;
    this.linkListener = new NetworkSenderLinkListener(preconfiguredMap, idFactory);
    this.codec = codec;
    this.connectingQueueSizeThreshold = connectingQueueSizeThreshold;
    this.connectingQueueWaitTime = connectingQueueWaitTime;
    this.executor = Executors.newFixedThreadPool(
        numThreads, new DefaultThreadFactory(NetworkSenderStage.class.getName()));
    this.linkMap = new ConcurrentHashMap<>();
    this.queueMap = new HashMap<>();
    this.readWriteLock = new ReentrantReadWriteLock();
    if (connectingQueueWaitTime <= 0) {
      throw new NetworkRuntimeException("Invalid connectingQueueWaitTime " + connectingQueueWaitTime);
    }
  }

  @Override
  public void onNext(final NetworkServiceEvent event) {
    final Link<NetworkServiceEvent> link = linkMap.get(event.remoteAddress());
    if (link != null) {
      try {
        link.write(event);
      } catch (RuntimeException e) {
        onException(e, event);
      }
    } else {
      putToConnectingQueue(event);
    }
  }

  private void writeData(final Link<NetworkServiceEvent> link, final NetworkServiceEvent event) {
    if (link == null) {
      onException(new IOException("Attempts to write with null link"), event);
    } else {
      link.write(event);
    }
  }

  private ExpirableConcurrentQueue<NetworkServiceEvent> putIfAbsent(
      final SocketAddress address, final ExpirableConcurrentQueue<NetworkServiceEvent> queue) {
    readWriteLock.writeLock().lock();
    try {
      if (!queueMap.containsKey(address)) {
        return queueMap.put(address, queue);
      } else {
        return queueMap.get(address);
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private void putToConnectingQueue(final NetworkServiceEvent event) {
    readWriteLock.readLock().lock();
    final ExpirableConcurrentQueue<NetworkServiceEvent> currentQueue = queueMap.get(event.remoteAddress());
    readWriteLock.readLock().unlock();
    final boolean isConnecting;

    final ExpirableConcurrentQueue<NetworkServiceEvent> ret;
    final ExpirableConcurrentQueue<NetworkServiceEvent> queue;

    if (currentQueue == null) {
      final ExpirableConcurrentQueue<NetworkServiceEvent> newQueue = new ExpirableConcurrentQueue<>();
      ret = putIfAbsent(event.remoteAddress(), newQueue);
      queue = ret == null ? newQueue : ret;
      isConnecting = ret != null;
    } else {
      isConnecting = true;
      queue = currentQueue;
    }

    if (queue.size() > connectingQueueSizeThreshold) {
      try {
        LOG.log(Level.FINE, "Connecting queue's size exceeds threshold " + connectingQueueSizeThreshold
            + Thread.currentThread().getName() + "will be waiting " + connectingQueueWaitTime +"msecs");
        Thread.sleep(connectingQueueWaitTime);
      } catch (InterruptedException e) {
        throw new NetworkRuntimeException("Unexpected InterruptedException.", e);
      }

      if (!queue.isExpired()) {
        onNext(event);
      } else {
        if (queue.isConnected()) {
          writeData(linkMap.get(event.remoteAddress()), event);
        } else {
          onException(new IOException("Failed to connect"), event);
        }
      }

      return;
    }

    readWriteLock.readLock().lock();
    final boolean isExpired = queue.isExpired();
    if (!isExpired) {
      queue.add(event);
    }
    readWriteLock.readLock().unlock();

    if (!isExpired) {
      if (!isConnecting) {
        requestConnection(event.remoteAddress());
      }
    } else {
      putToConnectingQueue(event);
    }
  }

  private void requestConnection(final SocketAddress address) {
    LOG.log(Level.FINE, "Request connection to " + address);
    final SenderConnectTask task = new SenderConnectTask(
        new SenderConnectCallable(address),
        new SenderAfterConnectHandler()
    );

    executor.submit(task);
  }

  private void onException(Throwable cause, NetworkServiceEvent event) {
    preconfiguredMap.getLinkListener(event.getEventClassNameCode())
        .onException(cause, idFactory.getNewInstance(event.getLocalId()), event.getDataList());
  }

  /**
   * Closes the stage
   */
  @Override
  public void close() throws Exception {
    LOG.log(Level.FINE, "close");

    if (this.executor != null) {
      this.executor.shutdown();
      try {
        // wait for threads to finish for timeout
        if (!executor.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
          LOG.log(Level.WARNING, "Executor did not terminate in {0} ms.", SHUTDOWN_TIMEOUT);
          List<Runnable> droppedRunnables = executor.shutdownNow();
          LOG.log(Level.WARNING, "Executor dropped {0} tasks.", droppedRunnables.size());
        }
      } catch (final InterruptedException e) {
        LOG.log(Level.WARNING, "Close interrupted", e);
        throw new NetworkRuntimeException(e);
      }
    }
  }

  final class SenderConnectCallable implements Callable<LinkReference> {

    private SocketAddress remoteAddress;

    SenderConnectCallable(final SocketAddress remoteAddress) {
      this.remoteAddress = remoteAddress;
    }

    @Override
    public LinkReference call() throws Exception {
      try {
        final Link<NetworkServiceEvent> link = transport.open(
            remoteAddress,
            codec,
            linkListener
        );
        return new LinkReference(link);
      } catch (Exception e) {
        return new LinkReference(remoteAddress);
      }
    }
  }

  final class SenderAfterConnectHandler implements EventHandler<LinkReference> {
    @Override
    public void onNext(LinkReference value) {

      readWriteLock.writeLock().lock();
      final ExpirableConcurrentQueue<NetworkServiceEvent> queue = queueMap.remove(value.getRemoteAddress());
      queue.expire();
      readWriteLock.writeLock().unlock();

      if (!value.isConnected()) {
        LOG.log(Level.FINE, "Error occurred while connecting.");
        NetworkServiceEvent event;
        while ((event = queue.poll()) != null) {
          onException(new IOException("Failed to connect"), event);
        }
      } else {
        queue.setConnected(true);
        linkMap.put(value.getRemoteAddress(), value.getLink());
        LOG.log(Level.FINE, "Start to consume queue for" + value.getRemoteAddress());
        NetworkServiceEvent event;
        while ((event = queue.poll()) != null) {
          writeData(value.getLink(), event);
        }
        LOG.log(Level.FINE, "Finish consuming queue");
      }
    }
  }
}

final class ExpirableConcurrentQueue<T> {

  private final List<T> queue;
  private boolean isExpired;
  private final AtomicBoolean isConnected;

  ExpirableConcurrentQueue() {
    this.queue = Collections.synchronizedList(new LinkedList<T>());
    this.isExpired = false;
    this.isConnected = new AtomicBoolean(false);
  }

  public boolean isConnected() {
    return isConnected.get();
  }

  public void setConnected(boolean connected) {
    isConnected.set(connected);
  }

  public boolean isExpired() {
    return isExpired;
  }

  public int size() {
    return queue.size();
  }

  public void expire() {
    isExpired = true;
  }

  public void add(T item) {
    queue.add(item);
  }

  public T poll() {
    if (queue.size() == 0) {
      return null;
    }

    return queue.remove(0);
  }
}

final class LinkReference {
  private final Link<NetworkServiceEvent> link;
  private final SocketAddress address;

  LinkReference(SocketAddress address) {
    this.address = address;
    this.link = null;
  }

  LinkReference(final Link<NetworkServiceEvent> link) {
    this.link = link;
    this.address = link.getRemoteAddress();
  }

  public SocketAddress getRemoteAddress() {
    return address;
  }

  public boolean isConnected() {
    return link != null;
  }

  public Link<NetworkServiceEvent> getLink() {
    return link;
  }
}

final class SenderConnectTask extends FutureTask<LinkReference> {

  private final EventHandler<LinkReference> handler;

  SenderConnectTask(
      final Callable<LinkReference> callable,
      final EventHandler<LinkReference> handler) {
    super(callable);
    this.handler = handler;
  }

  @Override
  public void done() {
    try {
      handler.onNext(get());
    } catch (Exception e) {
      throw new NetworkRuntimeException("All types of exceptions should be caught by NetworkExceptionHandler", e);
    }
  }
}

final class NetworkSenderLinkListener implements LinkListener<NetworkServiceEvent> {

  private final NetworkPreconfiguredMap preconfiguredMap;
  private final IdentifierFactory idFactory;

  NetworkSenderLinkListener(
      final NetworkPreconfiguredMap preconfiguredMap,
      final IdentifierFactory idFactory) {
    this.preconfiguredMap = preconfiguredMap;
    this.idFactory = idFactory;
  }

  @Override
  public void onSuccess(NetworkServiceEvent message) {
    preconfiguredMap.getLinkListener(message.getEventClassNameCode())
        .onSuccess(idFactory.getNewInstance(message.getLocalId()), message.getDataList());
  }

  @Override
  public void onException(Throwable cause, SocketAddress remoteAddress, NetworkServiceEvent message) {
    preconfiguredMap.getLinkListener(message.getEventClassNameCode())
        .onException(cause, idFactory.getNewInstance(message.getLocalId()), message.getDataList());
  }
}
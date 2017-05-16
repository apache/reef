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

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Encoder;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.netty.LoggingLinkListener;

import java.net.SocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Remote sender event handler.
 *
 * @param <T> type
 */
class RemoteSenderEventHandler<T> implements EventHandler<RemoteEvent<T>> {

  private static final Logger LOG = Logger.getLogger(RemoteSenderEventHandler.class.getName());

  private final BlockingQueue<RemoteEvent<T>> queue = new LinkedBlockingQueue<>();
  private final AtomicReference<Link<byte[]>> linkRef = new AtomicReference<>();

  private final RemoteEventEncoder<T> encoder;
  private final Transport transport;
  private final ExecutorService executor;

  /**
   * Constructs a remote sender event handler.
   *
   * @param encoder   the encoder
   * @param transport the transport to send events
   * @param executor  the executor service used for creating channels
   */
  RemoteSenderEventHandler(final Encoder<T> encoder, final Transport transport, final ExecutorService executor) {
    this.encoder = new RemoteEventEncoder<>(encoder);
    this.transport = transport;
    this.executor = executor;
  }

  @Override
  public String toString() {
    return String.format("RemoteSenderEventHandler: { transport: %s encoder: %s}", this.transport, this.encoder);
  }

  void setLink(final Link<byte[]> link) {
    LOG.log(Level.FINEST, "thread {0} set link {1}", new Object[] {Thread.currentThread(), link});
    linkRef.compareAndSet(null, link);
    consumeQueue();
  }

  void consumeQueue() {
    try {
      RemoteEvent<T> event;
      while ((event = queue.poll(0, TimeUnit.MICROSECONDS)) != null) {
        LOG.log(Level.FINEST, "Event: {0}", event);
        linkRef.get().write(encoder.encode(event));
      }
    } catch (final InterruptedException ex) {
      LOG.log(Level.SEVERE, "Interrupted", ex);
      throw new RemoteRuntimeException(ex);
    }
  }

  /**
   * Handles the event to send to a remote node.
   *
   * @param value the event
   * @throws RemoteRuntimeException
   */
  @Override
  public void onNext(final RemoteEvent<T> value) {
    try {

      LOG.log(Level.FINEST, "Link: {0} event: {1}", new Object[] {linkRef, value});

      if (linkRef.get() == null) {
        queue.add(value);

        final Link<byte[]> link = transport.get(value.remoteAddress());
        if (link != null) {
          LOG.log(Level.FINEST, "transport get link: {0}", link);
          setLink(link);
          return;
        }

        final ConnectFutureTask<Link<byte[]>> cf = new ConnectFutureTask<>(
            new ConnectCallable(transport, value.localAddress(), value.remoteAddress()),
            new ConnectEventHandler<>(this));
        executor.submit(cf);

      } else {
        // encode and write bytes
        // consumeQueue();
        LOG.log(Level.FINEST, "Send: {0} event: {1}", new Object[] {linkRef, value});
        linkRef.get().write(encoder.encode(value));
      }

    } catch (final RemoteRuntimeException ex) {
      LOG.log(Level.SEVERE, "Remote Exception", ex);
      throw ex;
    }
  }
}

class ConnectCallable implements Callable<Link<byte[]>> {

  private final Transport transport;
  private final SocketAddress remoteAddress;

  ConnectCallable(final Transport transport, final SocketAddress localAddress, final SocketAddress remoteAddress) {
    this.transport = transport;
    this.remoteAddress = remoteAddress;
  }

  @Override
  public Link<byte[]> call() throws Exception {
    return transport.open(remoteAddress,
        new ByteCodec(),
        new LoggingLinkListener<byte[]>());
  }
}

class ConnectEventHandler<T> implements EventHandler<ConnectFutureTask<Link<byte[]>>> {

  private static final Logger LOG = Logger.getLogger(ConnectEventHandler.class.getName());

  private final RemoteSenderEventHandler<T> handler;

  ConnectEventHandler(final RemoteSenderEventHandler<T> handler) {
    this.handler = handler;
  }

  @Override
  public void onNext(final ConnectFutureTask<Link<byte[]>> value) {
    try {
      handler.setLink(value.get());
    } catch (final InterruptedException | ExecutionException ex) {
      LOG.log(Level.SEVERE, "Execution Exception", ex);
      throw new RemoteRuntimeException(ex);
    }
  }
}

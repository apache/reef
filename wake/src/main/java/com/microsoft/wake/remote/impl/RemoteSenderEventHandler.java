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
package com.microsoft.wake.remote.impl;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.Encoder;
import com.microsoft.wake.remote.exception.RemoteRuntimeException;
import com.microsoft.wake.remote.transport.Link;
import com.microsoft.wake.remote.transport.Transport;
import com.microsoft.wake.remote.transport.netty.LoggingLinkListener;

/**
 * Remote sender event handler
 * 
 * @param <T> type
 */
class RemoteSenderEventHandler<T> implements EventHandler<RemoteEvent<T>> {

  private static final Logger LOG = Logger.getLogger(RemoteSenderEventHandler.class.getName());

  private final RemoteEventEncoder<T> encoder;
  private final Transport transport;
  private final BlockingQueue<RemoteEvent<T>> queue;
  private final AtomicReference<Link<byte[]>> linkRef;
  private final ExecutorService executor;

  /**
   * Constructs a remote sender event handler
   *  
   * @param encoder the encoder
   * @param transport the transport to send events
   * @param executor the executor service used for creating channels
   */
  RemoteSenderEventHandler(Encoder<T> encoder, Transport transport, ExecutorService executor) {
    this.encoder = new RemoteEventEncoder<T> (encoder);
    this.transport = transport;
    this.executor = executor;
    this.linkRef = new AtomicReference<Link<byte[]>>();
    this.queue = new LinkedBlockingQueue<RemoteEvent<T>>();
  }

  void setLink(Link<byte[]> link) {
    LOG.log(Level.FINEST, "thread {0} link {1}", new Object[]{Thread.currentThread(), link});
    linkRef.compareAndSet(null, link);
    consumeQueue();
  }
  
  void consumeQueue() {
    try {
      RemoteEvent<T> event;
      while((event = queue.poll(0, TimeUnit.MICROSECONDS)) != null) {
        LOG.log(Level.FINEST, "{0}", event);
        linkRef.get().write(encoder.encode(event));
      }
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
      throw new RemoteRuntimeException(e);
    }
  }
  
  /**
   * Handles the event to send to a remote node
   * 
   * @param value the event
   * @throws RemoteRuntimeException
   */
  @Override
  public void onNext(RemoteEvent<T> value) {
    try {
      if (linkRef.get() == null) {
        queue.add(value);

        Link<byte[]> link = transport.get(value.remoteAddress());
        if (link != null) {
          LOG.log(Level.FINEST, "transport get link: {0}", link);
          setLink(link);
          return;
        }
            
        ConnectFutureTask<Link<byte[]>> cf = new ConnectFutureTask<Link<byte[]>>(
                new ConnectCallable(transport, value.localAddress(), value.remoteAddress()),
                new ConnectEventHandler<T>(this));
        executor.submit(cf);
      } else {
        // encode and write bytes
        // consumeQueue();

        if (LOG.isLoggable(Level.FINEST)) 
          LOG.log(Level.FINEST, "Send an event from " + linkRef.get().getLocalAddress() + " to " + linkRef.get().getRemoteAddress() + " value " + value);
        linkRef.get().write(encoder.encode(value));
      }
    } catch (IOException ex) {
      ex.printStackTrace();
      throw new RemoteRuntimeException(ex);
    } catch (RemoteRuntimeException ex2) {
      ex2.printStackTrace();
      throw ex2;  
    }
  }
  

}

class ConnectCallable implements Callable<Link<byte[]>> {
  
  private final Transport transport;
  private final SocketAddress localAddress;
  private final SocketAddress remoteAddress;
  
  ConnectCallable(Transport transport, SocketAddress localAddress, SocketAddress remoteAddress) {
    this.transport = transport;
    this.localAddress = localAddress;
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

  private final RemoteSenderEventHandler<T> handler;
  
  ConnectEventHandler(RemoteSenderEventHandler<T> handler) {
    this.handler = handler;
  }
  
  @Override
  public void onNext(ConnectFutureTask<Link<byte[]>> value) {
    try {
      handler.setLink(value.get());
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      throw new RemoteRuntimeException(e);
    }
  }
  
}

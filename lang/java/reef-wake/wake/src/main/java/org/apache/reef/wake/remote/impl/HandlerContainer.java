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
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.RemoteIdentifier;
import org.apache.reef.wake.remote.RemoteMessage;
import org.apache.reef.wake.remote.exception.RemoteRuntimeException;
import org.apache.reef.wake.remote.transport.Transport;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main logic to dispatch messages.
 */
final class HandlerContainer<T> implements EventHandler<RemoteEvent<byte[]>> {

  private static final Logger LOG = Logger.getLogger(HandlerContainer.class.getName());

  private final ConcurrentMap<Class<? extends T>,
      EventHandler<RemoteMessage<? extends T>>> msgTypeToHandlerMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<Tuple2<RemoteIdentifier,
      Class<? extends T>>, EventHandler<? extends T>> tupleToHandlerMap = new ConcurrentHashMap<>();
  private final Codec<T> codec;
  private final String name;
  private Transport transport;

  HandlerContainer(final String name, final Codec<T> codec) {
    this.name = name;
    this.codec = codec;
  }

  void setTransport(final Transport transport) {
    this.transport = transport;
  }

  @SuppressWarnings("checkstyle:diamondoperatorforvariabledefinition")
  public AutoCloseable registerHandler(final RemoteIdentifier sourceIdentifier,
                                       final Class<? extends T> messageType,
                                       final EventHandler<? extends T> theHandler) {


    final Tuple2<RemoteIdentifier, Class<? extends T>> tuple =
        new Tuple2<RemoteIdentifier, Class<? extends T>>(sourceIdentifier, messageType);

    final EventHandler<? extends T> prevHandler =
        this.tupleToHandlerMap.putIfAbsent(tuple, theHandler);

    if (prevHandler != null) {
      this.tupleToHandlerMap.replace(tuple, theHandler);
    }

    LOG.log(Level.FINER, "{0}", tuple);
    return new Subscription(tuple, this);
  }

  public AutoCloseable registerHandler(
      final Class<? extends T> messageType,
      final EventHandler<RemoteMessage<? extends T>> theHandler) {

    final EventHandler<RemoteMessage<? extends T>> prevHandler =
        this.msgTypeToHandlerMap.putIfAbsent(messageType, theHandler);

    if (prevHandler != null) {
      this.msgTypeToHandlerMap.replace(messageType, theHandler);
    }

    LOG.log(Level.FINER, "{0}", messageType);
    return new Subscription(messageType, this);
  }

  public AutoCloseable registerErrorHandler(final EventHandler<Exception> theHandler) {
    this.transport.registerErrorHandler(theHandler);
    return new Subscription(new Exception("Token for finding the error handler subscription"), this);
  }

  /**
   * Unsubscribes a handler.
   *
   * @param subscription
   * @throws org.apache.reef.wake.remote.exception.RemoteRuntimeException if the Subscription type is unknown
   */
  public void unsubscribe(final Subscription<T> subscription) {
    final T token = subscription.getToken();
    LOG.log(Level.FINER, "RemoteManager: {0} token {1}", new Object[]{this.name, token});
    if (token instanceof Exception) {
      this.transport.registerErrorHandler(null);
    } else if (token instanceof Tuple2) {
      this.tupleToHandlerMap.remove(token);
    } else if (token instanceof Class) {
      this.msgTypeToHandlerMap.remove(token);
    } else {
      throw new RemoteRuntimeException(
          "Unknown subscription type: " + subscription.getClass().getName());
    }
  }

  /**
   * Dispatches a message.
   *
   * @param value
   */
  @SuppressWarnings("checkstyle:diamondoperatorforvariabledefinition")
  @Override
  public synchronized void onNext(final RemoteEvent<byte[]> value) {

    LOG.log(Level.FINER, "RemoteManager: {0} value: {1}", new Object[]{this.name, value});

    final T decodedEvent = this.codec.decode(value.getEvent());
    final Class<?> clazz = decodedEvent.getClass();

    // check remote identifier and message type
    final SocketRemoteIdentifier id =
        new SocketRemoteIdentifier((InetSocketAddress) value.remoteAddress());

    final Tuple2<RemoteIdentifier, Class<?>> tuple = new Tuple2<RemoteIdentifier, Class<?>>(id, clazz);

    final EventHandler<T> tupleHandler = (EventHandler<T>) this.tupleToHandlerMap.get(tuple);
    if (tupleHandler != null) {
      LOG.log(Level.FINER, "Tuple handler: {0}", tuple);
      tupleHandler.onNext(decodedEvent);
    } else {
      final EventHandler<RemoteMessage<? extends T>> messageHandler =
          this.msgTypeToHandlerMap.get(clazz);
      if (messageHandler != null) {
        LOG.log(Level.FINER, "Message handler: {0}", clazz);
        messageHandler.onNext(new DefaultRemoteMessage(id, decodedEvent));
      } else {
        final RuntimeException ex = new RemoteRuntimeException(
            "Unknown message type in dispatch: " + clazz.getName() + " from " + id);
        LOG.log(Level.WARNING, "Unknown message type in dispatch.", ex);
        throw ex;
      }
    }
  }
}

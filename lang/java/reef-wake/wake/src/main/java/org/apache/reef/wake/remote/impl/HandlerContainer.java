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
 * An event handler that receives a remote message with a binary payload,
 * decodes a message from the blob, and dispatches that message to a proper handler.
 */
final class HandlerContainer<T> implements EventHandler<RemoteEvent<byte[]>> {

  private static final Logger LOG = Logger.getLogger(HandlerContainer.class.getName());

  private final ConcurrentMap<Class<? extends T>,
      EventHandler<RemoteMessage<? extends T>>> msgTypeToHandlerMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<Tuple2<RemoteIdentifier, Class<? extends T>>,
      EventHandler<? extends T>> tupleToHandlerMap = new ConcurrentHashMap<>();

  private final Codec<T> codec;
  private final String name;

  private Transport transport;

  HandlerContainer(final String name, final Codec<T> codec) {

    this.name = name;
    this.codec = codec;

    LOG.log(Level.FINER, "Instantiated {0}", this);
  }

  @Override
  public String toString() {
    return String.format("HandlerContainer: {name:%s codec:%s}",
        this.name, this.codec.getClass().getCanonicalName());
  }

  void setTransport(final Transport transport) {
    this.transport = transport;
  }

  /**
   * Subscribe for events from a given source and message type.
   * @param sourceIdentifier An identifier of an event source.
   * @param messageType Java class of messages to dispatch.
   * @param theHandler Message handler.
   * @return A new subscription object that will cancel its subscription on .close()
   */
  @SuppressWarnings("checkstyle:diamondoperatorforvariabledefinition")
  public AutoCloseable registerHandler(
      final RemoteIdentifier sourceIdentifier,
      final Class<? extends T> messageType,
      final EventHandler<? extends T> theHandler) {

    final Tuple2<RemoteIdentifier, Class<? extends T>> tuple =
        new Tuple2<RemoteIdentifier, Class<? extends T>>(sourceIdentifier, messageType);

    this.tupleToHandlerMap.put(tuple, theHandler);

    LOG.log(Level.FINER,
        "Add handler for tuple: {0},{1}",
        new Object[] {tuple.getT1(), tuple.getT2().getCanonicalName()});

    return new SubscriptionHandler<>(tuple, this.unsubscribeTuple);
  }

  /**
   * Subscribe for events of a given message type.
   * @param messageType Java class of messages to dispatch.
   * @param theHandler Message handler.
   * @return A new subscription object that will cancel its subscription on .close()
   */
  public AutoCloseable registerHandler(
      final Class<? extends T> messageType,
      final EventHandler<RemoteMessage<? extends T>> theHandler) {

    this.msgTypeToHandlerMap.put(messageType, theHandler);

    LOG.log(Level.FINER, "Add handler for class: {0}", messageType.getCanonicalName());

    return new SubscriptionHandler<>(messageType, this.unsubscribeClass);
  }

  /**
   * Specify handler for error messages.
   * @param theHandler Error handler.
   * @return A new subscription object that will cancel its subscription on .close()
   */
  public AutoCloseable registerErrorHandler(final EventHandler<Exception> theHandler) {
    this.transport.registerErrorHandler(theHandler);
    return new SubscriptionHandler<>(
        new Exception("Token for finding the error handler subscription"), this.unsubscribeException);
  }

  /**
   * Unsubscribes a handler.
   *
   * @param subscription
   * @throws org.apache.reef.wake.remote.exception.RemoteRuntimeException if the Subscription type is unknown.
   * @deprecated [REEF-1544] Prefer using SubscriptionHandler and the corresponding methods
   * instead of the old Subscription class. Remove method after release 0.16.
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
          "Unknown subscription type: " + subscription.getClass().getCanonicalName());
    }
  }

  /** Unsubscribe from messages of a given class. */
  private final SubscriptionHandler.Unsubscriber<Class<? extends T>>
      unsubscribeClass = new SubscriptionHandler.Unsubscriber<Class<? extends T>>() {
        @Override
        public void unsubscribe(final Class<? extends T> token) {
          LOG.log(Level.FINER, "Unsubscribe: {0} class {1}", new Object[] {name, token.getCanonicalName()});
          msgTypeToHandlerMap.remove(token);
        }
      };

  /** Unsubscribe from event from a certain source and message type. */
  private final SubscriptionHandler.Unsubscriber<Tuple2<RemoteIdentifier, Class<? extends T>>>
      unsubscribeTuple = new SubscriptionHandler.Unsubscriber<Tuple2<RemoteIdentifier, Class<? extends T>>>() {
        @Override
        public void unsubscribe(final Tuple2<RemoteIdentifier, Class<? extends T>> token) {
          LOG.log(Level.FINER, "Unsubscribe: {0} tuple {1},{2}",
              new Object[] {name, token.getT1(), token.getT2().getCanonicalName()});
          tupleToHandlerMap.remove(token);
        }
      };

  /** Unsubscribe from error messages. */
  private final SubscriptionHandler.Unsubscriber<Exception>
      unsubscribeException = new SubscriptionHandler.Unsubscriber<Exception>() {
        @Override
        public void unsubscribe(final Exception token) {
          LOG.log(Level.FINER, "Unsubscribe: {0} exception {1}", new Object[] {name, token});
          transport.registerErrorHandler(null);
        }
      };

  /**
   * Dispatch message received from the remote to proper event handler.
   * @param value Remote message, encoded as byte[].
   */
  @Override
  @SuppressWarnings("checkstyle:diamondoperatorforvariabledefinition")
  public synchronized void onNext(final RemoteEvent<byte[]> value) {

    LOG.log(Level.FINER, "RemoteManager: {0} value: {1}", new Object[] {this.name, value});

    final T decodedEvent = this.codec.decode(value.getEvent());
    final Class<?> clazz = decodedEvent.getClass();

    LOG.log(Level.FINEST, "RemoteManager: {0} decoded event {1} :: {2}",
        new Object[] {this.name, clazz.getCanonicalName(), decodedEvent});

    // check remote identifier and message type
    final SocketRemoteIdentifier id = new SocketRemoteIdentifier((InetSocketAddress)value.remoteAddress());

    final Tuple2<RemoteIdentifier, Class<?>> tuple = new Tuple2<RemoteIdentifier, Class<?>>(id, clazz);

    final EventHandler<T> tupleHandler = (EventHandler<T>) this.tupleToHandlerMap.get(tuple);

    if (tupleHandler != null) {

      LOG.log(Level.FINER, "Tuple handler: {0},{1}",
          new Object[] {tuple.getT1(), tuple.getT2().getCanonicalName()});

      tupleHandler.onNext(decodedEvent);

    } else {

      final EventHandler<RemoteMessage<? extends T>> messageHandler = this.msgTypeToHandlerMap.get(clazz);

      if (messageHandler == null) {
        final RuntimeException ex = new RemoteRuntimeException(
            "Unknown message type in dispatch: " + clazz.getCanonicalName() + " from " + id);
        LOG.log(Level.WARNING, "Unknown message type in dispatch.", ex);
        throw ex;
      }

      LOG.log(Level.FINER, "Message handler: {0}", clazz.getCanonicalName());

      messageHandler.onNext(new DefaultRemoteMessage(id, decodedEvent));
    }
  }
}

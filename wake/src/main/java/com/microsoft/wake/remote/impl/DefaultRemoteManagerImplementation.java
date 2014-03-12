/**
 * Copyright (C) 2012 Microsoft Corporation
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

import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.StageManager;
import com.microsoft.wake.remote.*;
import com.microsoft.wake.remote.exception.RemoteRuntimeException;
import com.microsoft.wake.remote.transport.Transport;
import com.microsoft.wake.remote.transport.netty.NettyMessagingTransport;

import javax.inject.Inject;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default remote manager implementation
 */
public class DefaultRemoteManagerImplementation implements RemoteManager {

  private static final Logger LOG = Logger.getLogger(HandlerContainer.class.getName());

  private static final AtomicInteger counter = new AtomicInteger(0);

  /**
   * The timeout used for the execute running in close()
   */
  private static final long CLOSE_EXECUTOR_TIMEOUT = 10000; //ms

  private RemoteIdentifier myIdentifier;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final String name;
  private final Codec<?> codec;
  private final Transport transport;
  private final RemoteSenderStage reSendStage;
  private final EStage<TransportEvent> reRecvStage;
  private final HandlerContainer handlerContainer;
  private final RemoteSeqNumGenerator seqGen = new RemoteSeqNumGenerator();


  /**
   * Constructs a remote manager
   *
   * @param hostAddress
   * @param listeningPort
   * @param codec
   * @param errorHandler
   */
  @Inject
  public <T> DefaultRemoteManagerImplementation(
      final @Parameter(RemoteConfiguration.ManagerName.class) String name,
      final @Parameter(RemoteConfiguration.HostAddress.class) String hostAddress,
      final @Parameter(RemoteConfiguration.Port.class) int listeningPort,
      final @Parameter(RemoteConfiguration.MessageCodec.class) Codec<T> codec,
      final @Parameter(RemoteConfiguration.ErrorHandler.class) EventHandler<Throwable> errorHandler,
      final @Parameter(RemoteConfiguration.OrderingGuarantee.class) boolean orderingGuarantee) {

    this.name = name;
    this.codec = codec;
    this.handlerContainer = new HandlerContainer<>(name, codec);

    this.reRecvStage = orderingGuarantee ?
      new OrderedRemoteReceiverStage(handlerContainer, errorHandler) :
      new RemoteReceiverStage(handlerContainer, errorHandler);

    this.transport = new NettyMessagingTransport(
        hostAddress, listeningPort, this.reRecvStage, this.reRecvStage);

    this.handlerContainer.setTransport(this.transport);

    this.myIdentifier = new SocketRemoteIdentifier(
        (InetSocketAddress)this.transport.getLocalAddress());

    this.reSendStage = new RemoteSenderStage(codec, transport);

    StageManager.instance().register(this);

    LOG.log(Level.FINEST, "RemoteManager {0} instantiated id {1} counter {2}",
        new Object[] { this.name, this.myIdentifier, counter.incrementAndGet() });
  }

  @Inject
  public <T> DefaultRemoteManagerImplementation(
      final @Parameter(RemoteConfiguration.ManagerName.class) String name,
      final @Parameter(RemoteConfiguration.Port.class) int listeningPort,
      final @Parameter(RemoteConfiguration.MessageCodec.class) Codec<T> codec,
      final @Parameter(RemoteConfiguration.ErrorHandler.class) EventHandler<Throwable> errorHandler,
      final @Parameter(RemoteConfiguration.OrderingGuarantee.class) boolean orderingGuarantee)
      throws UnknownHostException {
    this(name, NetUtils.getLocalAddress(), listeningPort, codec, errorHandler, orderingGuarantee);
  }

  @Inject
  public <T> DefaultRemoteManagerImplementation(
      final @Parameter(RemoteConfiguration.ManagerName.class) String name,
      final @Parameter(RemoteConfiguration.MessageCodec.class) Codec<T> codec,
      final @Parameter(RemoteConfiguration.ErrorHandler.class) EventHandler<Throwable> errorHandler,
      final @Parameter(RemoteConfiguration.OrderingGuarantee.class) boolean orderingGuarantee)
      throws UnknownHostException {
    this(name, NetUtils.getLocalAddress(), 0, codec, errorHandler, orderingGuarantee);
  }

  @Inject
  public <T> DefaultRemoteManagerImplementation(
      final @Parameter(RemoteConfiguration.ManagerName.class) String name,
      final @Parameter(RemoteConfiguration.Port.class) int listeningPort,
      final @Parameter(RemoteConfiguration.ErrorHandler.class) EventHandler<Throwable> errorHandler,
      final @Parameter(RemoteConfiguration.OrderingGuarantee.class) boolean orderingGuarantee)
      throws UnknownHostException {
    this(name, NetUtils.getLocalAddress(), listeningPort,
        new ObjectSerializableCodec<>(), errorHandler, orderingGuarantee);
  }

  @Inject
  public DefaultRemoteManagerImplementation(
      final @Parameter(RemoteConfiguration.ManagerName.class) String name,
      final @Parameter(RemoteConfiguration.ErrorHandler.class) EventHandler<Throwable> errorHandler,
      final @Parameter(RemoteConfiguration.OrderingGuarantee.class) boolean orderingGuarantee)
      throws UnknownHostException {
    this(name, new ObjectSerializableCodec<>(), errorHandler, orderingGuarantee);
  }

  /**
   * Returns a proxy event handler for a remote identifier and a message type
   *
   * @param <T>
   * @param destinationIdentifier
   * @param messageType
   */
  @Override
  public <T> EventHandler<T> getHandler(
      final RemoteIdentifier destinationIdentifier, final Class<? extends T> messageType) {

    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "RemoteManager: {0} destinationIdentifier: {1} messageType: {2}",
          new Object[]{this.name, destinationIdentifier, messageType.getName()});
    }

    return new ProxyEventHandler<>(this.myIdentifier, destinationIdentifier,
        "default", this.reSendStage.<T>getHandler(), this.seqGen);
  }

  /**
   * Registers an event handler for a remote identifier and a message type and
   * returns a subscription
   *
   * @param <T,              U extends T>
   * @param sourceIdentifier
   * @param messageType
   * @param theHandler
   */
  @Override
  public <T, U extends T> AutoCloseable registerHandler(final RemoteIdentifier sourceIdentifier,
        final Class<U> messageType, final EventHandler<T> theHandler) {
    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "RemoteManager: {0} remoteid: {1} messageType: {2} handler: {3}",
          new Object[]{this.name, sourceIdentifier, messageType.getName(),
              theHandler.getClass().getName()});
    }
    return handlerContainer.registerHandler(sourceIdentifier, messageType, theHandler);
  }

  /**
   * Registers an event handler for a message type and returns a subscription
   *
   * @param <T,         U extends T>
   * @param messageType
   * @param theHandler
   */
  @Override
  public <T, U extends T> AutoCloseable registerHandler(
      final Class<U> messageType, final EventHandler<RemoteMessage<T>> theHandler) {
    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "RemoteManager: {0} messageType: {1} handler: {2}",
          new Object[]{this.name, messageType.getName(), theHandler.getClass().getName()});
    }
    return handlerContainer.registerHandler(messageType, theHandler);
  }

  /**
   * Registers an exception handler and returns a subscription
   *
   * @param theHandler
   */
  @Override
  public AutoCloseable registerErrorHandler(final EventHandler<Exception> theHandler) {
    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "RemoteManager: {0} handler: {1}",
          new Object[]{this.name, theHandler.getClass().getName()});
    }
    return handlerContainer.registerErrorHandler(theHandler);
  }

  /**
   * Returns my identifier
   */
  @Override
  public RemoteIdentifier getMyIdentifier() {
    return myIdentifier;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {

      LOG.log(Level.FINE, "RemoteManager: {0} Closing remote manager id: {1}",
          new Object[]{this.name, myIdentifier});

      final Runnable closeRunnable = new Runnable() {
        @Override
        public void run() {
          try {
            LOG.log(Level.FINE, "Closing sender stage {0}", myIdentifier);
            reSendStage.close();
            LOG.log(Level.FINE, "Closed the remote sender stage");
          } catch (final Exception e) {
            LOG.log(Level.SEVERE, "Unable to close the remote sender stage", e);
          }

          try {
            LOG.log(Level.FINE, "Closing transport {0}", myIdentifier);
            transport.close();
            LOG.log(Level.FINE, "Closed the transport");
          } catch (final Exception e) {
            LOG.log(Level.SEVERE, "Unable to close the transport.", e);
          }

          try {
            LOG.log(Level.FINE, "Closing receiver stage {0}", myIdentifier);
            reRecvStage.close();
            LOG.log(Level.FINE, "Closed the remote receiver stage");
          } catch (final Exception e) {
            LOG.log(Level.SEVERE, "Unable to close the remote receiver stage", e);
          }
        }

      };

      final ExecutorService closeExecutor = Executors.newSingleThreadExecutor();
      closeExecutor.submit(closeRunnable);
      closeExecutor.shutdown();
      if (!closeExecutor.isShutdown()) {
        LOG.log(Level.SEVERE, "close executor did not shutdown properly.");
      }

      final long endTime = System.currentTimeMillis() + CLOSE_EXECUTOR_TIMEOUT;
      while (!closeExecutor.isTerminated()) {
        try {
          final long waitTime = endTime - System.currentTimeMillis();
          closeExecutor.awaitTermination(waitTime, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
        }
      }

      if (closeExecutor.isTerminated()) {
        LOG.log(Level.FINE, "close executor did terminate properly.");
      } else {
        LOG.log(Level.SEVERE, "close executor did not terminate properly.");
      }
    }
  }
}

final class HandlerContainer<T> implements EventHandler<RemoteEvent<byte[]>> {

  private static final Logger LOG = Logger.getLogger(HandlerContainer.class.getName());

  private final ConcurrentMap<Class<? extends T>,
      EventHandler<RemoteMessage<? extends T>>> msgTypeToHandlerMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<Tuple2<RemoteIdentifier,
      Class<? extends T>>, EventHandler<? extends T>> tupleToHandlerMap = new ConcurrentHashMap<>();

  private Transport transport;
  private final Codec<T> codec;
  private final String name;

  HandlerContainer(final String name, final Codec<T> codec) {
    this.name = name;
    this.codec = codec;
  }

  void setTransport(final Transport transport) {
    this.transport = transport;
  }

  public AutoCloseable registerHandler(final RemoteIdentifier sourceIdentifier,
      final Class<? extends T> messageType, final EventHandler<? extends T> theHandler) {

    final Tuple2<RemoteIdentifier, Class<? extends T>> tuple =
        new Tuple2<RemoteIdentifier, Class<? extends T>>(sourceIdentifier, messageType);

    final EventHandler<? extends T> handler = tupleToHandlerMap.putIfAbsent(tuple, theHandler);
    if (handler != null) {
      tupleToHandlerMap.replace(tuple, theHandler);
    }

    LOG.log(Level.FINER, "{0}", tuple);
    return new Subscription(tuple, this);
  }

  public AutoCloseable registerHandler(
      final Class<? extends T> messageType,
      final EventHandler<RemoteMessage<? extends T>> theHandler) {

    final EventHandler<RemoteMessage<? extends T>> handler =
        msgTypeToHandlerMap.put(messageType, theHandler);

    if (handler != null) {
      msgTypeToHandlerMap.replace(messageType, theHandler);
    }

    LOG.log(Level.FINER, "{0}", messageType);
    return new Subscription(messageType, this);
  }

  public AutoCloseable registerErrorHandler(final EventHandler<Exception> theHandler) {
    transport.registerErrorHandler(theHandler);
    return new Subscription(new Exception(), this);
  }

  /**
   * Unsubscribes a handler
   *
   * @param subscription
   * @throws RemoteRuntimeException if the Subscription type is unknown
   */
  public void unsubscribe(final Subscription<T> subscription) {
    final T token = subscription.getToken();
    LOG.log(Level.FINER, "RemoteManager: {0} token {1}", new Object[]{this.name, token});
    if (token instanceof Exception) {
      transport.registerErrorHandler(null);
    } else if (token instanceof Tuple2 || token instanceof Class) {
      tupleToHandlerMap.remove(token);
    } else {
      throw new RemoteRuntimeException(
          "Unknown subscription type: " + subscription.getClass().getName());
    }
  }

  /**
   * Dispatches a message
   *
   * @param value
   */
  @Override
  public synchronized void onNext(final RemoteEvent<byte[]> value) {

    LOG.log(Level.FINER, "RemoteManager: {0} value: {1}", new Object[]{this.name, value});

    final T obj = codec.decode(value.getEvent());
    final Class<?> clazz = obj.getClass();

    // check remote identifier and message type
    final SocketRemoteIdentifier id = new SocketRemoteIdentifier((InetSocketAddress) value.remoteAddress());
    final Tuple2<RemoteIdentifier, Class<?>> tuple = new Tuple2<RemoteIdentifier, Class<?>>(id, clazz);

    final EventHandler<T> handler = (EventHandler<T>) tupleToHandlerMap.get(tuple);
    if (handler != null) {
      LOG.log(Level.FINER, "handler1 {0}", tuple);
      handler.onNext(codec.decode(value.getEvent()));
    } else {
      final EventHandler<RemoteMessage<? extends T>> handler2 = msgTypeToHandlerMap.get(clazz);
      if (handler2 != null) {
        LOG.log(Level.FINER, "handler2 {0}", clazz);
        handler2.onNext(new DefaultRemoteMessage(id, codec.decode(value.getEvent())));
      } else {
        final RuntimeException ex = new RemoteRuntimeException(
            "Unknown message type in dispatch: " + clazz.getName() + " from " + id);
        LOG.log(Level.WARNING, "Unknown message type in dispatch.", ex);
        throw ex;
      }
    }
  }
}

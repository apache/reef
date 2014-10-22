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

import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.StageManager;
import com.microsoft.wake.remote.*;
import com.microsoft.wake.remote.transport.Transport;
import com.microsoft.wake.remote.transport.netty.NettyMessagingTransport;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
  private final Transport transport;
  private final RemoteSenderStage reSendStage;
  private final EStage<TransportEvent> reRecvStage;
  private final HandlerContainer handlerContainer;
  private final RemoteSeqNumGenerator seqGen = new RemoteSeqNumGenerator();

  @Inject
  public <T> DefaultRemoteManagerImplementation(
      final @Parameter(RemoteConfiguration.ManagerName.class) String name,
      final @Parameter(RemoteConfiguration.HostAddress.class) String hostAddress,
      final @Parameter(RemoteConfiguration.Port.class) int listeningPort,
      final @Parameter(RemoteConfiguration.MessageCodec.class) Codec<T> codec,
      final @Parameter(RemoteConfiguration.ErrorHandler.class) EventHandler<Throwable> errorHandler,
      final @Parameter(RemoteConfiguration.OrderingGuarantee.class) boolean orderingGuarantee,
      final @Parameter(RemoteConfiguration.NumberOfTries.class) int numberOfTries,
      final @Parameter(RemoteConfiguration.RetryTimeout.class) int retryTimeout) {

    this.name = name;
    this.handlerContainer = new HandlerContainer<>(name, codec);

    this.reRecvStage = orderingGuarantee ?
        new OrderedRemoteReceiverStage(this.handlerContainer, errorHandler) :
        new RemoteReceiverStage(this.handlerContainer, errorHandler, 10);

    if ("##UNKNOWN##".equals(hostAddress)) {
      this.transport = new NettyMessagingTransport(
          NetUtils.getLocalAddress(), listeningPort, this.reRecvStage, this.reRecvStage, numberOfTries, retryTimeout);
    } else {
      this.transport = new NettyMessagingTransport(
          hostAddress, listeningPort, this.reRecvStage, this.reRecvStage, numberOfTries, retryTimeout);
    }

    this.handlerContainer.setTransport(this.transport);

    this.myIdentifier = new SocketRemoteIdentifier(
        (InetSocketAddress) this.transport.getLocalAddress());

    this.reSendStage = new RemoteSenderStage(codec, this.transport, 10);

    StageManager.instance().register(this);

    LOG.log(Level.FINEST, "RemoteManager {0} instantiated id {1} counter {2} listening on {3}:{4}",
        new Object[]{this.name, this.myIdentifier, counter.incrementAndGet(),
            this.transport.getLocalAddress().toString(),
            this.transport.getListeningPort()}
    );
  }

  /**
   * Returns a proxy event handler for a remote identifier and a message type
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
   */
  @Override
  public <T, U extends T> AutoCloseable registerHandler(
      final RemoteIdentifier sourceIdentifier,
      final Class<U> messageType, final EventHandler<T> theHandler) {
    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "RemoteManager: {0} remoteid: {1} messageType: {2} handler: {3}",
          new Object[]{this.name, sourceIdentifier, messageType.getName(),
              theHandler.getClass().getName()});
    }
    return this.handlerContainer.registerHandler(sourceIdentifier, messageType, theHandler);
  }

  /**
   * Registers an event handler for a message type and returns a subscription
   */
  @Override
  public <T, U extends T> AutoCloseable registerHandler(
      final Class<U> messageType, final EventHandler<RemoteMessage<T>> theHandler) {
    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "RemoteManager: {0} messageType: {1} handler: {2}",
          new Object[]{this.name, messageType.getName(), theHandler.getClass().getName()});
    }
    return this.handlerContainer.registerHandler(messageType, theHandler);
  }

  /**
   * Registers an exception handler and returns a subscription
   */
  @Override
  public AutoCloseable registerErrorHandler(final EventHandler<Exception> theHandler) {
    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "RemoteManager: {0} handler: {1}",
          new Object[]{this.name, theHandler.getClass().getName()});
    }
    return this.handlerContainer.registerErrorHandler(theHandler);
  }

  /**
   * Returns my identifier
   */
  @Override
  public RemoteIdentifier getMyIdentifier() {
    return this.myIdentifier;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {

      LOG.log(Level.FINE, "RemoteManager: {0} Closing remote manager id: {1}",
          new Object[]{this.name, this.myIdentifier});

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
          LOG.log(Level.FINE, "Interrupted", e);
        }
      }

      if (closeExecutor.isTerminated()) {
        LOG.log(Level.FINE, "Close executor terminated properly.");
      } else {
        LOG.log(Level.SEVERE, "Close executor did not terminate properly.");
      }
    }
  }
}

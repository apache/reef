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
package com.microsoft.reef.runtime.common.driver.client;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.parameters.ClientCloseHandlers;
import com.microsoft.reef.driver.parameters.ClientCloseWithMessageHandlers;
import com.microsoft.reef.driver.parameters.ClientMessageHandlers;
import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.DriverStatusManager;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.utils.BroadCastEventHandler;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents the Client in the Driver.
 */
@Private
@DriverSide
public final class ClientManager implements EventHandler<ClientRuntimeProtocol.JobControlProto> {

  private final static Logger LOG = Logger.getLogger(ClientManager.class.getName());


  private final InjectionFuture<Set<EventHandler<Void>>> clientCloseHandlers;

  private final InjectionFuture<Set<EventHandler<byte[]>>> clientCloseWithMessageHandlers;

  private final InjectionFuture<Set<EventHandler<byte[]>>> clientMessageHandlers;

  private final DriverStatusManager driverStatusManager;

  private volatile EventHandler<Void> clientCloseDispatcher;

  private volatile EventHandler<byte[]> clientCloseWithMessageDispatcher;

  private volatile EventHandler<byte[]> clientMessageDispatcher;


  @Inject
  ClientManager(final @Parameter(ClientCloseHandlers.class) InjectionFuture<Set<EventHandler<Void>>> clientCloseHandlers,
                final @Parameter(ClientCloseWithMessageHandlers.class) InjectionFuture<Set<EventHandler<byte[]>>> clientCloseWithMessageHandlers,
                final @Parameter(ClientMessageHandlers.class) InjectionFuture<Set<EventHandler<byte[]>>> clientMessageHandlers,
                final @Parameter(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.class) String clientRID,
                final RemoteManager remoteManager,
                final DriverStatusManager driverStatusManager) {
    this.driverStatusManager = driverStatusManager;
    this.clientCloseHandlers = clientCloseHandlers;
    this.clientCloseWithMessageHandlers = clientCloseWithMessageHandlers;
    this.clientMessageHandlers = clientMessageHandlers;

    if (!clientRID.equals(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.NONE)) {
      remoteManager.registerHandler(clientRID, ClientRuntimeProtocol.JobControlProto.class, this);
    } else {
      LOG.log(Level.FINE, "Not registering a handler for JobControlProto, as there is no client.");
    }
  }

  /**
   * This method reacts to control messages passed by the client to the driver. It will forward
   * messages related to the ClientObserver interface to the Driver. It will also initiate a shutdown
   * if the client indicates a close message.
   *
   * @param jobControlProto contains the client initiated control message
   */
  @Override
  public synchronized void onNext(final ClientRuntimeProtocol.JobControlProto jobControlProto) {
    if (jobControlProto.hasSignal()) {
      if (jobControlProto.getSignal() == ClientRuntimeProtocol.Signal.SIG_TERMINATE) {
        try {
          if (jobControlProto.hasMessage()) {
            getClientCloseWithMessageDispatcher().onNext(jobControlProto.getMessage().toByteArray());
          } else {
            getClientCloseDispatcher().onNext(null);
          }
        } finally {
          this.driverStatusManager.onComplete();
        }
      } else {
        LOG.log(Level.FINEST, "Unsupported signal: " + jobControlProto.getSignal());
      }
    } else if (jobControlProto.hasMessage()) {
      getClientMessageDispatcher().onNext(jobControlProto.getMessage().toByteArray());
    }
  }

  private synchronized EventHandler<Void> getClientCloseDispatcher() {
    if (clientCloseDispatcher != null) {
      return clientCloseDispatcher;
    } else {
      synchronized (this) {
        if (clientCloseDispatcher == null)
          clientCloseDispatcher = new BroadCastEventHandler<>(clientCloseHandlers.get());
      }
      return clientCloseDispatcher;
    }
  }

  private EventHandler<byte[]> getClientCloseWithMessageDispatcher() {
    if (clientCloseWithMessageDispatcher != null) {
      return clientCloseWithMessageDispatcher;
    } else {
      synchronized (this) {
        if (clientCloseWithMessageDispatcher == null)
          clientCloseWithMessageDispatcher = new BroadCastEventHandler<>(clientCloseWithMessageHandlers.get());
      }
      return clientCloseWithMessageDispatcher;
    }
  }

  private EventHandler<byte[]> getClientMessageDispatcher() {
    if (clientMessageDispatcher != null) {
      return clientMessageDispatcher;
    } else {
      synchronized (this) {
        if (clientMessageDispatcher == null)
          clientMessageDispatcher = new BroadCastEventHandler<>(clientMessageHandlers.get());
      }
      return clientMessageDispatcher;
    }
  }

}

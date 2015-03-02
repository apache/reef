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

import org.apache.reef.io.network.*;
import org.apache.reef.io.network.NetworkService;
import org.apache.reef.io.network.exception.NetworkRuntimeException;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.naming.exception.NamingException;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.NetUtils;
import org.apache.reef.wake.remote.impl.*;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default network service implementation
 */
public final class DefaultNetworkServiceImplementation implements NetworkService {

  private static final Logger LOG = Logger.getLogger(DefaultNetworkServiceImplementation.class.getName());

  private final Transport transport;
  private final EStage<TransportEvent> receiverStage;
  private final EStage<NetworkServiceEvent> senderStage;
  private final NetworkPreconfiguredMap preconfiguredMap;
  private final NamingProxy namingProxy;
  private final boolean transferSenderIdentifier;

  @Inject
  public DefaultNetworkServiceImplementation(
      final NetworkPreconfiguredMap networkPreconfiguredMap,
      final NamingProxy namingProxy,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory idFactory,
      final @Parameter(NetworkServiceParameter.SenderRetryCount.class) int retryCount,
      final @Parameter(NetworkServiceParameter.SenderRetryTimeout.class) int retryTimeout,
      final @Parameter(NetworkServiceParameter.NetworkServiceEventCodec.class) Codec<NetworkServiceEvent> eventCodec,
      final @Parameter(NetworkServiceParameter.NetworkReceiveErrorHandler.class) EventHandler<Throwable> errorHandler,
      final @Parameter(NetworkServiceParameter.NetworkServicePort.class) int nsPort,
      final @Parameter(NetworkServiceParameter.NetworkServiceIdentifier.class) String serviceId,
      final @Parameter(NetworkServiceParameter.NetworkReceiverThreadNumber.class) int receiverThreadNum,
      final @Parameter(NetworkServiceParameter.NetworkSenderThreadNumber.class) int senderThreadNum,
      final @Parameter(NetworkServiceParameter.TransferSenderIdentifier.class) boolean transferSenderIdentifier,
      final @Parameter(NetworkServiceParameter.ConnectingQueueSizeThreshold.class) int connectingQueueSizeThreshold,
      final @Parameter(NetworkServiceParameter.ConnectingQueueWaitingTime.class) int connectingQueueWaitingTime) {

    this.namingProxy = namingProxy;
    this.preconfiguredMap = networkPreconfiguredMap;
    this.transferSenderIdentifier = transferSenderIdentifier;
    this.receiverStage = new NetworkReceiverStage(
        new NetworkReceiverEventHandler(
            idFactory,
            preconfiguredMap,
            eventCodec
        ),
        errorHandler,
        receiverThreadNum);
    this.transport = new NettyMessagingTransport(NetUtils.getLocalAddress(), nsPort,
        receiverStage, receiverStage, retryCount, retryTimeout);
    this.senderStage = new NetworkSenderStage(transport, eventCodec, idFactory, networkPreconfiguredMap,
        senderThreadNum, connectingQueueSizeThreshold, connectingQueueWaitingTime);

    final Identifier networkServiceId = idFactory.getNewInstance(serviceId);
    namingProxy.registerMyId(networkServiceId, (InetSocketAddress) transport.getLocalAddress());
    LOG.log(Level.INFO, networkServiceId + " is registered in NameServer.");
  }

  @Override
  public Identifier getLocalId() {
    return namingProxy.getLocalIdentifier();
  }

  @Override
  public <T> void sendEvent(final Identifier remoteId, final T event) {
    final List<T> eventList = new ArrayList<>(1);
    eventList.add(event);
    sendEventList(remoteId, eventList);
  }

  @Override
  public <T> void sendEventList(final Identifier remoteId, final List<T> eventList) {
    if (eventList.size() == 0) {
      throw new NetworkRuntimeException("Sends empty event list to " + remoteId);
    }

    final String eventClassName = eventList.get(0).getClass().getName();
    final InetSocketAddress remoteAddress = lookupRemoteAddress(remoteId, eventList, eventClassName);
    if (remoteAddress != null) {
      send(remoteId, remoteAddress, eventList, eventClassName);
    }
  }

  private <T> InetSocketAddress lookupRemoteAddress(final Identifier remoteId, final List<T> eventList,
                                                    final String eventClassName) {
    try {
      InetSocketAddress remoteAddress = namingProxy.lookup(remoteId);
      if (remoteAddress == null) {
        throw new NamingException("Cannot find \"" + remoteId + "\" from the name server");
      }
      return remoteAddress;
    } catch (Exception e) {
      preconfiguredMap.<T>getLinkListener(eventClassName)
          .onException(e, remoteId, eventList);
      return null;
    }
  }

  private <T> void send(final Identifier remoteId, final InetSocketAddress remoteAddress,
                        final List<T> eventList, final String eventClassName) {
    final NetworkServiceEvent<T> serviceEvent;

    if (transferSenderIdentifier) {
      serviceEvent = new NetworkServiceEvent<>(
          preconfiguredMap.getEventClassNameCode(eventClassName),
          eventList,
          namingProxy.getLocalIdentifier().toString(),
          remoteId.toString()
      );
    } else {
      serviceEvent = new NetworkServiceEvent<>(
          preconfiguredMap.getEventClassNameCode(eventClassName),
          eventList,
          null,
          remoteId.toString()
      );
    }

    serviceEvent.setRemoteAddress(remoteAddress);
    senderStage.onNext(serviceEvent);
  }

  @Override
  public SocketAddress getLocalAddress() {
    return this.transport.getLocalAddress();
  }

  @Override
  public NamingProxy getNamingProxy() {
    return namingProxy;
  }

  @Override
  public void close() throws Exception {
    LOG.log(Level.FINE, "Shutting down");
    this.namingProxy.unregisterMyId();
    this.transport.close();
    this.namingProxy.close();
    this.receiverStage.close();
    this.senderStage.close();
  }
}

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
package org.apache.reef.services.network.util;

import org.apache.reef.io.network.NetworkEventHandler;
import org.apache.reef.io.network.NetworkLinkListener;
import org.apache.reef.io.network.NetworkServiceParameter;
import org.apache.reef.io.network.impl.*;
import org.apache.reef.io.network.naming.NameLookupClient;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import java.util.HashSet;
import java.util.Set;

public final class NetworkServiceUtil {
  private static final int retryCount;
  private static final int retryTimeout;
  private static final int nameLookupRetryCount;
  private static final int nameLookupRetryTimeout;
  private static final long cacheTimeout;
  private static final long requestTimeout;
  private static final int receiverNum;
  private static final int senderNum;
  private static final int connectThreshold;
  private static final int connectWaitingTime;

  static {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector();
      retryCount = injector.getNamedInstance(NetworkServiceParameter.SenderRetryCount.class);
      retryTimeout = injector.getNamedInstance(NetworkServiceParameter.SenderRetryTimeout.class);
      nameLookupRetryCount = injector.getNamedInstance(NameLookupClient.RetryCount.class);
      nameLookupRetryTimeout = injector.getNamedInstance(NameLookupClient.RetryTimeout.class);
      cacheTimeout = injector.getNamedInstance(NameLookupClient.CacheTimeout.class);
      requestTimeout = injector.getNamedInstance(NameLookupClient.RequestTimeout.class);
      senderNum = injector.getNamedInstance(NetworkServiceParameter.NetworkSenderThreadNumber.class);
      receiverNum = injector.getNamedInstance(NetworkServiceParameter.NetworkReceiverThreadNumber.class);
      connectThreshold = injector.getNamedInstance(NetworkServiceParameter.ConnectingQueueSizeThreshold.class);
      connectWaitingTime = injector.getNamedInstance(NetworkServiceParameter.ConnectingQueueWaitingTime.class);
    } catch (final InjectionException ex) {
      final String msg = "Exception while trying to find default values for retryCount & Timeout";
      throw new RuntimeException(msg, ex);
    }
  }

  public static org.apache.reef.io.network.NetworkService getTestNetworkService(
      Set<String> events,
      Set<NetworkEventHandler<?>> eventHandlers,
      Set<Codec<?>> eventCodecs,
      Set<NetworkLinkListener<?>> linkListeners,
      String serviceId,
      int nameServerPort) {

    if (linkListeners == null) {
      linkListeners = new HashSet<>();
    }

    final LocalAddressProvider addressProvider;
    try {
      addressProvider = Tang.Factory.getTang().newInjector().getInstance(LocalAddressProvider.class);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }

    NetworkPreconfiguredMap map = new NetworkPreconfiguredMap(events, eventHandlers, eventCodecs, linkListeners);

    NameClientProxy namingProxy = new NameClientProxy(addressProvider.getLocalAddress(),
        nameServerPort, requestTimeout,
        new StringIdentifierFactory(),
        nameLookupRetryCount, nameLookupRetryTimeout, addressProvider);
    Codec<NetworkServiceEvent> networkServiceEventCodec;
    networkServiceEventCodec = new NetworkServiceEventCodec(map);

    return new DefaultNetworkServiceImplementation(
        map,
        namingProxy,
        new StringIdentifierFactory(),
        retryCount,
        retryTimeout,
        networkServiceEventCodec,
        new ExceptionHandler(),
        0,
        serviceId,
        receiverNum,
        senderNum,
        false,
        connectThreshold,
        connectWaitingTime
    );
  }
}

/**
 * Test exception handler
 */
final class ExceptionHandler implements EventHandler<Throwable> {
  @Override
  public void onNext(Throwable error) {
    System.err.println(error);
  }
}

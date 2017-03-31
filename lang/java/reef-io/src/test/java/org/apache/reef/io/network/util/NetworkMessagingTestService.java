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
package org.apache.reef.io.network.util;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.impl.config.NetworkConnectionServiceIdFactory;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.LinkListener;
import org.junit.Assert;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class for NetworkConnectionService test.
 */
public final class NetworkMessagingTestService implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(NetworkMessagingTestService.class.getName());

  private final IdentifierFactory factory;
  private final NetworkConnectionService receiverNetworkConnService;
  private final NetworkConnectionService senderNetworkConnService;
  private final NameServer nameServer;

  public NetworkMessagingTestService(final String localAddress) throws InjectionException {
    // name server
    final Injector injector = Tang.Factory.getTang().newInjector();
    this.nameServer = injector.getInstance(NameServer.class);
    final Configuration netConf = NameResolverConfiguration.CONF
        .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, localAddress)
        .set(NameResolverConfiguration.NAME_SERVICE_PORT, nameServer.getPort())
        .build();

    LOG.log(Level.FINEST, "=== Test network connection service receiver start");
    // network service for receiver
    final Injector injectorReceiver = injector.forkInjector(netConf);
    this.receiverNetworkConnService = injectorReceiver.getInstance(NetworkConnectionService.class);
    this.factory = injectorReceiver.getNamedInstance(NetworkConnectionServiceIdFactory.class);

    // network service for sender
    LOG.log(Level.FINEST, "=== Test network connection service sender start");
    final Injector injectorSender = injector.forkInjector(netConf);
    senderNetworkConnService = injectorSender.getInstance(NetworkConnectionService.class);
  }

  public <T> void registerTestConnectionFactory(final Identifier connFactoryId,
                                                final int numMessages, final Monitor monitor,
                                                final Codec<T> codec) throws NetworkException {
    final Identifier receiverEndPointId = factory.getNewInstance("receiver");
    final Identifier senderEndPointId = factory.getNewInstance("sender");
    receiverNetworkConnService.registerConnectionFactory(connFactoryId, codec,
        new MessageHandler<T>(monitor, numMessages, senderEndPointId, receiverEndPointId),
        new TestListener<T>(), receiverEndPointId);
    senderNetworkConnService.registerConnectionFactory(connFactoryId, codec,
        new MessageHandler<T>(monitor, numMessages, receiverEndPointId, senderEndPointId),
        new TestListener<T>(), senderEndPointId);
  }

  public <T> Connection<T> getConnectionFromSenderToReceiver(final Identifier connFactoryId) {
    final Identifier receiverEndPointId = factory.getNewInstance("receiver");
    return (Connection<T>)senderNetworkConnService
        .getConnectionFactory(connFactoryId)
        .newConnection(receiverEndPointId);
  }

  public void close() throws Exception {
    senderNetworkConnService.close();
    receiverNetworkConnService.close();
    nameServer.close();
  }

  public static final class MessageHandler<T> implements EventHandler<Message<T>> {

    private final int expected;
    private final Monitor monitor;
    private final Identifier expectedSrcId;
    private final Identifier expectedDestId;
    private final AtomicInteger count = new AtomicInteger(0);

    public MessageHandler(final Monitor monitor,
                          final int expected,
                          final Identifier expectedSrcId,
                          final Identifier expectedDestId) {
      this.monitor = monitor;
      this.expected = expected;
      this.expectedSrcId = expectedSrcId;
      this.expectedDestId = expectedDestId;
    }

    @Override
    public void onNext(final Message<T> value) {

      final int currentCount = count.incrementAndGet();
      LOG.log(Level.FINER, "Message {0}/{1} :: {2}", new Object[] {currentCount, expected, value});

      Assert.assertEquals(expectedSrcId, value.getSrcId());
      Assert.assertEquals(expectedDestId, value.getDestId());
      Assert.assertTrue(currentCount <= expected);

      if (currentCount >= expected) {
        monitor.mnotify();
      }
    }
  }

  public static final class TestListener<T> implements LinkListener<Message<T>> {
    @Override
    public void onSuccess(final Message<T> message) {
      LOG.log(Level.FINER, "Success: message {0}", message);
    }
    @Override
    public void onException(final Throwable cause, final SocketAddress remoteAddress, final Message<T> message) {
      LOG.log(Level.WARNING, "Exception: message " + message, cause);
      throw new RuntimeException(cause);
    }
  }
}

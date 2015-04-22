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

import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.NamingProxy;
import org.apache.reef.io.network.naming.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.impl.SingleThreadStage;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * NamingProxy using NameClient
 */
public final class NameClientProxy implements NamingProxy {

  private static final Logger LOG = Logger.getLogger(NameClientProxy.class.getName());

  private final int serverProt;
  private final EStage<Tuple<Identifier, InetSocketAddress>> nameServiceRegisteringStage;
  private final EStage<Identifier> nameServiceUnregisteringStage;
  private final NameClient nameClient;
  private Tuple<Identifier, InetSocketAddress> myId;

  @Inject
  public NameClientProxy(
      final @Parameter(NameServerParameters.NameServerAddr.class) String serverAddr,
      final @Parameter(NameServerParameters.NameServerPort.class) int serverPort,
      final @Parameter(NameLookupClient.CacheTimeout.class) long timeout,
      final @Parameter(NameServerParameters.NameServerIdentifierFactory.class) IdentifierFactory factory,
      final @Parameter(NameLookupClient.RetryCount.class) int retryCount,
      final @Parameter(NameLookupClient.RetryTimeout.class) int retryTimeout,
      final LocalAddressProvider addressProvider) {

    this.serverProt = serverPort;
    this.nameClient = new NameClient(serverAddr, serverProt, factory, retryCount,
        retryTimeout, new NameCache(timeout), addressProvider);
    this.nameServiceRegisteringStage = new SingleThreadStage<>(
            "NameServiceRegisterer", new EventHandler<Tuple<Identifier, InetSocketAddress>>() {
      @Override
      public void onNext(final Tuple<Identifier, InetSocketAddress> tuple) {
        try {
          nameClient.register(tuple.getKey(), tuple.getValue());
          LOG.log(Level.FINEST, "Registered {0} with nameservice", tuple.getKey());
        } catch (final Exception ex) {
          final String msg = "Unable to register " + tuple.getKey() + "with name service";
          LOG.log(Level.WARNING, msg, ex);
          throw new RuntimeException(msg, ex);
        }
      }
    }, 5);

    this.nameServiceUnregisteringStage = new SingleThreadStage<>(
            "NameServiceUnregisterer", new EventHandler<Identifier>() {
      @Override
      public void onNext(final Identifier id) {
        try {
          nameClient.unregister(id);
          LOG.log(Level.FINEST, "Unregistered {0} with nameservice", id);
        } catch (final Exception ex) {
          final String msg = "Unable to unregister " + id + " with name service";
          LOG.log(Level.WARNING, msg, ex);
          throw new RuntimeException(msg, ex);
        }
      }
    }, 5);
  }

  @Override
  public Identifier getLocalIdentifier() {
    if(myId == null)
      return null;
    return this.myId.getKey();
  }

  @Override
  public int getNameServerPort() {
    return serverProt;
  }

  @Override
  public InetSocketAddress getLocalAddress() {
    if(myId == null)
      return null;
    return this.myId.getValue();
  }

  @Override
  public void close() throws Exception {
    nameClient.close();
  }

  @Override
  public InetSocketAddress lookup(Identifier id) throws Exception {
    return nameClient.lookup(id);
  }

  @Override
  public void registerMyId(Identifier id, InetSocketAddress addr) {
    final Tuple<Identifier, InetSocketAddress> tuple =
            new Tuple<>(id, addr);
    myId = tuple;
    LOG.log(Level.FINEST, "Binding {0} to NetworkService@({1})",
        new Object[]{tuple.getKey(), tuple.getValue()});
    this.nameServiceRegisteringStage.onNext(tuple);
  }

  @Override
  public void unregisterMyId() {
    if (this.myId == null) {
      LOG.log(Level.WARNING, "The identifier was already removed.");
      return;
    }

    LOG.log(Level.FINEST, "Unbinding {0} to NetworkService@({1})",
            new Object[]{this.myId.getKey(), this.myId.getValue()});
    this.nameServiceUnregisteringStage.onNext(myId.getKey());
    this.myId = null;
  }
}

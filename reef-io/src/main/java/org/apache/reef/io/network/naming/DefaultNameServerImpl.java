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
package org.apache.reef.io.network.naming;

import org.apache.reef.io.naming.NameAssignment;
import org.apache.reef.io.network.naming.serialization.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.Stage;
import org.apache.reef.wake.impl.MultiEventHandler;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.NetUtils;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;
import org.apache.reef.webserver.AvroReefServiceInfo;
import org.apache.reef.webserver.ReefEventStateManager;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Naming server implementation
 */
public class DefaultNameServerImpl implements NameServer {

  private static final Logger LOG = Logger.getLogger(DefaultNameServerImpl.class.getName());

  @Inject
  public DefaultNameServerImpl() {
    LOG.log(Level.INFO, "DefaultNameServerImpl is used. No name server is registered");
  }

  /**
   * Gets port
   */
  @Override
  public int getPort() {
    return 9999;
  }

  /**
   * Closes resources
   */
  @Override
  public void close() throws Exception {
  }

  /**
   * Registers an (identifier, address) mapping locally
   *
   * @param id   an identifier
   * @param addr an Internet socket address
   */
  @Override
  public void register(final Identifier id, final InetSocketAddress addr) {
  }

  /**
   * Unregisters an identifier locally
   *
   * @param id an identifier
   */
  @Override
  public void unregister(final Identifier id) {
  }

  /**
   * Finds an address for an identifier locally
   *
   * @param id an identifier
   * @return an Internet socket address
   */
  @Override
  public InetSocketAddress lookup(final Identifier id) {
    return null;
  }

  /**
   * Finds addresses for identifiers locally
   *
   * @param identifiers an iterable of identifiers
   * @return a list of name assignments
   */
  @Override
  public List<NameAssignment> lookup(final Iterable<Identifier> identifiers) {
    return null;
  }

}

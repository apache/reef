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

import org.apache.reef.io.network.NamingProxy;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * NamingProxy using NameServer
 */
public final class NameServerProxy implements NamingProxy {

  private static final Logger LOG = Logger.getLogger(NameServerProxy.class.getName());

  private final NameServer nameServer;
  private Identifier myId;
  private InetSocketAddress myAddress;

  @Inject
  public NameServerProxy(final NameServer nameServer){
    this.nameServer = nameServer;
  }

  @Override
  public Identifier getLocalIdentifier() {
    return myId;
  }

  @Override
  public int getNameServerPort(){
    return nameServer.getPort();
  }

  @Override
  public InetSocketAddress getLocalAddress() {
    return myAddress;
  }

  @Override
  public void close() throws Exception {
    nameServer.close();
  }

  @Override
  public InetSocketAddress lookup(Identifier id) throws Exception {
    return nameServer.lookup(id);
  }

  @Override
  public void registerMyId(Identifier id, InetSocketAddress addr) {
    this.myId = id;
    this.myAddress = addr;
    nameServer.register(id, addr);
  }

  @Override
  public void unregisterMyId() {
    if (this.myId == null) {
      LOG.log(Level.WARNING, "The identifier was already removed.");
      return;
    }

    nameServer.unregister(myId);
    myId = null;
  }
}

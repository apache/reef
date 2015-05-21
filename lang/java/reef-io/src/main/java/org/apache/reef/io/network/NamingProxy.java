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
package org.apache.reef.io.network;

import org.apache.reef.io.naming.NamingLookup;
import org.apache.reef.wake.Identifier;

import java.net.InetSocketAddress;

/**
 * Proxy registers and un-registers an identifier and can derive its address
 */
public interface NamingProxy extends NamingLookup, AutoCloseable {

  /**
   * Return the latest registered identifier through this proxy
   *
   * @return identifier of naming proxy
   */
  public Identifier getLocalIdentifier();

  /**
   * Return the latest registered socket address through this proxy
   *
   * @return socket address of naming proxy
   */
  public InetSocketAddress getLocalAddress();

  /**
   * NamingProxy must collaborate with a name server and this method
   * returns the port number required to establish a connection.
   *
   * @return port number of name server
   */
  public int getNameServerPort();

  /**
   * Registers local id
   *
   * @param myId
   */
  public void registerMyId(Identifier myId, InetSocketAddress myAddress);

  /**
   * Unregisters local id
   */
  public void unregisterMyId();
}

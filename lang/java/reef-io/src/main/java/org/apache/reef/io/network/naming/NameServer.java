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

package org.apache.reef.io.network.naming;

import org.apache.reef.io.naming.NameAssignment;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.Stage;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Naming server interface.
 */
@DefaultImplementation(NameServerImpl.class)
public interface NameServer extends Stage {

  /**
   * get port number.
   * @return
   */
  int getPort();

  /**
   * Registers an (identifier, address) mapping locally.
   *
   * @param id   an identifier
   * @param addr an Internet socket address
   */
  void register(Identifier id, InetSocketAddress addr);

  /**
   * Unregisters an identifier locally.
   *
   * @param id an identifier
   */
  void unregister(Identifier id);

  /**
   * Finds an address for an identifier locally.
   *
   * @param id an identifier
   * @return an Internet socket address
   */
  InetSocketAddress lookup(Identifier id);

  /**
   * Finds addresses for identifiers locally.
   *
   * @param identifiers an Iterable of identifiers
   * @return a list of name assignments
   */
  List<NameAssignment> lookup(Iterable<Identifier> identifiers);
}

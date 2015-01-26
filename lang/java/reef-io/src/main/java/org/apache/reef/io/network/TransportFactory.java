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

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Transport;

/**
 * Factory that creates a transport
 */
public interface TransportFactory {

  /**
   * Creates a transport
   *
   * @param port          a listening port
   * @param clientHandler a transport client-side handler
   * @param serverHandler a transport server-side handler
   * @param exHandler     an exception handler
   * @return
   */
  public Transport create(int port,
                          EventHandler<TransportEvent> clientHandler,
                          EventHandler<TransportEvent> serverHandler,
                          EventHandler<Exception> exHandler);
}

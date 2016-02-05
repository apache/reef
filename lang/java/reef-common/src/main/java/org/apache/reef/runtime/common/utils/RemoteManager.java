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
package org.apache.reef.runtime.common.utils;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.RemoteIdentifierFactory;
import org.apache.reef.wake.remote.RemoteMessage;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Wrapper for org.apache.reef.wake.remote.RemoteManager.
 */
public class RemoteManager {

  private static final Logger LOG = Logger.getLogger(RemoteManager.class.getName());

  private final org.apache.reef.wake.remote.RemoteManager raw;
  private final RemoteIdentifierFactory factory;

  @Inject
  public RemoteManager(final org.apache.reef.wake.remote.RemoteManager raw,
                       final RemoteIdentifierFactory factory) {
    this.raw = raw;
    this.factory = factory;
    LOG.log(Level.FINE, "Instantiated 'RemoteManager' with remoteId: {0}", this.getMyIdentifier());
  }

  public final org.apache.reef.wake.remote.RemoteManager raw() {
    return this.raw;
  }

  public void close() throws Exception {
    this.raw.close();
  }

  public <T> EventHandler<T> getHandler(
      final String destinationIdentifier, final Class<? extends T> messageType) {
    return this.raw.getHandler(factory.getNewInstance(destinationIdentifier), messageType);
  }

  public <T, U extends T> AutoCloseable registerHandler(
      final String sourceIdentifier, final Class<U> messageType,
      final EventHandler<T> theHandler) {
    return this.raw.registerHandler(factory.getNewInstance(sourceIdentifier), messageType, theHandler);
  }

  public <T, U extends T> AutoCloseable registerHandler(
      final Class<U> messageType, final EventHandler<RemoteMessage<T>> theHandler) {
    return this.raw.registerHandler(messageType, theHandler);
  }

  // TODO[JIRA REEF-547]: This method uses deprecated raw.registerErrorHandler.
  /**
   * @deprecated in 0.14, will be deleted in 0.15
   */
  @Deprecated
  public AutoCloseable registerErrorHandler(final EventHandler<Exception> theHandler) {
    return this.raw.registerErrorHandler(theHandler);
  }

  public String getMyIdentifier() {
    return this.raw.getMyIdentifier().toString();
  }
}


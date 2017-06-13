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
package org.apache.reef.runtime.spark.util;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.RemoteIdentifierFactory;
import org.apache.reef.wake.remote.RemoteManager;
import org.apache.reef.wake.remote.RemoteManagerFactory;
import org.apache.reef.wake.remote.RemoteMessage;

import javax.inject.Inject;

/**
 * Since the existing RemoteManager cannot use an additional codec,
 * we need this additional RemoteManager to use MesosMessageCodec.
 * TODO[JIRA REEF-103]: Replace this class once Tang's namespace feature is enabled
 */
public final class SparkRemoteManager {
  private final RemoteManager raw;
  private final RemoteIdentifierFactory factory;

  @Inject
  SparkRemoteManager(final RemoteIdentifierFactory factory,
                     final SparkErrorHandler sparkErrorHandler,
                     final SparkRemoteManagerCodec codec,
                     final RemoteManagerFactory remoteManagerFactory) {
    this.factory = factory;
    this.raw = remoteManagerFactory.getInstance("SPARK_EXECUTOR", codec, sparkErrorHandler);


  }

  public <T> EventHandler<T> getHandler(
      final String destinationIdentifier, final Class<? extends T> messageType) {
    return this.raw.getHandler(factory.getNewInstance(destinationIdentifier), messageType);
  }

  public <T, U extends T> AutoCloseable registerHandler(
      final Class<U> messageType, final EventHandler<RemoteMessage<T>> theHandler) {
    return this.raw.registerHandler(messageType, theHandler);
  }

  public String getMyIdentifier() {
    return this.raw.getMyIdentifier().toString();
  }
}

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
package org.apache.reef.io.network.impl;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A connection factory which is created by NetworkService.
 */
final class NSConnectionFactory<T> implements ConnectionFactory<T> {

  private final ConcurrentMap<Identifier, Connection<T>> connectionMap;
  private final String connFactoryId;
  private final Codec<T> eventCodec;
  private final EventHandler<Message<T>> eventHandler;
  private final LinkListener<Message<T>> eventListener;
  private final DefaultNetworkServiceClientImpl networkService;

  NSConnectionFactory(
      final DefaultNetworkServiceClientImpl networkService,
      final String connFactoryId,
      final Codec<T> eventCodec,
      final EventHandler<Message<T>> eventHandler,
      final LinkListener<Message<T>> eventListener) {
    this.networkService = networkService;
    this.connectionMap = new ConcurrentHashMap<>();
    this.connFactoryId = connFactoryId;
    this.eventCodec = eventCodec;
    this.eventHandler = eventHandler;
    this.eventListener = eventListener;
  }

  @Override
  public Connection<T> newConnection(final Identifier remoteId) {
    final Connection<T> connection = connectionMap.get(remoteId);

    if (connection == null) {
      final Connection<T> newConnection = new DefaultNSConnection<>(this, remoteId);
      connectionMap.putIfAbsent(remoteId, newConnection);
      return connectionMap.get(remoteId);
    }
    return connection;
  }

  <T> Link<DefaultNSMessage<T>> openLink(final Identifier remoteId) throws NetworkException {
    return networkService.openLink(remoteId);
  }

  String getConnectionFactoryId() {
    return this.connFactoryId;
  }

  Identifier getSrcId() {
    return this.networkService.getNetworkServiceClientId();
  }

  EventHandler<Message<T>> getEventHandler() {
    return eventHandler;
  }

  LinkListener<Message<T>> getLinkListener() {
    return eventListener;
  }

  public void removeConnection(final Identifier remoteId) {
    connectionMap.remove(remoteId);
  }

  Codec<T> getCodec() {
    return eventCodec;
  }
}
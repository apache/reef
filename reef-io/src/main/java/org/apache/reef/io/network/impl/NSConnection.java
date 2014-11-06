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

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.naming.exception.NamingException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A connection from the network service
 */
class NSConnection<T> implements Connection<T> {

  private static final Logger LOG = Logger.getLogger(NSConnection.class.getName());

  private final Identifier srcId;
  private final Identifier destId;
  private final LinkListener<NSMessage<T>> listener;
  private final NetworkService<T> service;
  private final Codec<NSMessage<T>> codec;

  // link can change when an endpoint physical address changes
  private Link<NSMessage<T>> link;

  /**
   * Constructs a connection
   *
   * @param srcId    a source identifier
   * @param destId   a destination identifier
   * @param listener a link listener
   * @param service  a network service
   */
  NSConnection(final Identifier srcId, final Identifier destId,
               final LinkListener<T> listener, final NetworkService<T> service) {
    this.srcId = srcId;
    this.destId = destId;
    this.listener = new NSMessageLinkListener<>(listener);
    this.service = service;
    this.codec = new NSMessageCodec<>(service.getCodec(), service.getIdentifierFactory());
  }

  /**
   * Opens the connection.
   */
  @Override
  public void open() throws NetworkException {

    LOG.log(Level.FINE, "looking up {0}", this.destId);

    try {
      // naming lookup
      final InetSocketAddress addr = this.service.getNameClient().lookup(this.destId);
      if (addr == null) {
        final NetworkException ex = new NamingException("Cannot resolve " + this.destId);
        LOG.log(Level.WARNING, "Cannot resolve " + this.destId, ex);
        throw ex;
      }

      LOG.log(Level.FINE, "Resolved {0} to {1}", new Object[]{this.destId, addr});

      // connect to a remote address
      this.link = this.service.getTransport().open(addr, this.codec, this.listener);
      LOG.log(Level.FINE, "Transport returned a link {0}", this.link);

    } catch (final Exception ex) {
      LOG.log(Level.WARNING, "Could not open " + this.destId, ex);
      throw new NetworkException(ex);
    }
  }

  /**
   * Writes an object to the connection
   *
   * @param obj an object of type T
   * @throws a network exception
   */
  @Override
  public void write(final T obj) throws NetworkException {
    try {
      this.link.write(new NSMessage<T>(this.srcId, this.destId, obj));
    } catch (final IOException ex) {
      LOG.log(Level.WARNING, "Could not write to " + this.destId, ex);
      throw new NetworkException(ex);
    }
  }

  /**
   * Closes the connection and unregisters it from the service
   */
  @Override
  public void close() throws NetworkException {
    this.service.remove(this.destId);
  }
}

/**
 * No-op link listener
 *
 * @param <T>
 */
final class NSMessageLinkListener<T> implements LinkListener<NSMessage<T>> {

  NSMessageLinkListener(final LinkListener<T> listener) {
  }

  @Override
  public void messageReceived(final NSMessage<T> message) {
  }
}

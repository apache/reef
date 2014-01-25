/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.impl;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.Connection;
import com.microsoft.reef.io.network.naming.exception.NamingException;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.remote.Codec;
import com.microsoft.wake.remote.transport.Link;
import com.microsoft.wake.remote.transport.LinkListener;

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
  NSConnection(final Identifier srcId, final Identifier destId, final LinkListener<T> listener,
               final NetworkService<T> service) {
    this.srcId = srcId;
    this.destId = destId;
    this.listener = new NSMessageLinkListener<T>(listener);
    this.service = service;
    this.codec = new NSMessageCodec<T>(service.getCodec(), service.getIdentifierFactory());
  }

  /**
   * Opens the connection
   *
   * @throws a network exception
   */
  @Override
  public void open() throws NetworkException {
    try {
      LOG.log(Level.FINE, "looking up " + destId);
      // naming lookup
      final InetSocketAddress addr = service.getNameClient().lookup(destId);
      if (addr == null)
        throw new NamingException("Cannot resolve " + destId);
      LOG.log(Level.FINE, "Resolved " + destId + " to " + addr);
      // connect to a remote address
      link = service.getTransport().open(addr, codec, listener);
      LOG.log(Level.FINE, "Transport returned a link " + link);
    } catch (Exception e) {
      throw new NetworkException(e.getCause());
    }
  }

  /**
   * Writes an object to the connection
   *
   * @param obj an object of type T
   * @throws a network exception
   */
  @Override
  public void write(T obj) throws NetworkException {
    try {
      link.write(new NSMessage<T>(srcId, destId, obj));
    } catch (final IOException e) {
      throw new NetworkException(e);
    }
  }

  /**
   * Closes the connection and unregisters it from the service
   */
  @Override
  public void close() throws NetworkException {
    service.remove(destId);
    try {
      link.close();
    } catch (Exception e) {
      throw new NetworkException("Unable to close the link.", e);
    }
  }

}

/**
 * No-op link listener
 *
 * @param <T>
 */
class NSMessageLinkListener<T> implements LinkListener<NSMessage<T>> {

  NSMessageLinkListener(LinkListener<T> listener) {
  }

  @Override
  public void messageReceived(NSMessage<T> message) {
  }

}

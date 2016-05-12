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

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.naming.exception.NamingException;
import org.apache.reef.io.network.naming.parameters.NameResolverCacheTimeout;
import org.apache.reef.io.network.naming.parameters.NameResolverRetryCount;
import org.apache.reef.io.network.naming.parameters.NameResolverRetryTimeout;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.cache.Cache;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * NameResolver looking up local name server.
 * This class should be used when the NameServer is started locally.
 */
public final class LocalNameResolverImpl implements NameResolver {

  private static final Logger LOG = Logger.getLogger(LocalNameResolverImpl.class.getName());

  /**
   * A local name server.
   */
  private final NameServer nameServer;

  /**
   * A cache for lookup.
   */
  private final Cache<Identifier, InetSocketAddress> cache;

  /**
   * Retry count for lookup.
   */
  private final int retryCount;

  /**
   * Retry timeout for lookup.
   */
  private final int retryTimeout;

  @Inject
  private LocalNameResolverImpl(
      final NameServer nameServer,
      @Parameter(NameResolverCacheTimeout.class) final long timeout,
      @Parameter(NameResolverRetryCount.class) final int retryCount,
      @Parameter(NameResolverRetryTimeout.class) final int retryTimeout) {
    this.nameServer = nameServer;
    this.cache = new NameCache(timeout);
    this.retryCount = retryCount;
    this.retryTimeout = retryTimeout;
  }

  @Override
  public synchronized void register(final Identifier id, final InetSocketAddress address) throws NetworkException {
    nameServer.register(id, address);
  }

  @Override
  public synchronized void unregister(final Identifier id) throws NetworkException {
    nameServer.unregister(id);
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public InetSocketAddress lookup(final Identifier id) throws Exception {
    return cache.get(id, new Callable<InetSocketAddress>() {
      @Override
      public InetSocketAddress call() throws Exception {
        final int origRetryCount = LocalNameResolverImpl.this.retryCount;
        int retriesLeft = origRetryCount;
        while (true) {
          try {
            final InetSocketAddress addr = nameServer.lookup(id);
            if (addr == null) {
              throw new NullPointerException("The lookup of the address in the nameServer returned null for id " + id);
            } else {
              return addr;
            }
          } catch (final NullPointerException e) {
            if (retriesLeft <= 0) {
              throw new NamingException("Cannot find " + id + " from the name server", e);
            } else {
              final int retTimeout = LocalNameResolverImpl.this.retryTimeout
                  * (origRetryCount - retriesLeft + 1);
              LOG.log(Level.WARNING,
                  "Caught Naming Exception while looking up " + id
                      + " with Name Server. Will retry " + retriesLeft
                      + " time(s) after waiting for " + retTimeout + " msec.");
              Thread.sleep(retTimeout);
              --retriesLeft;
            }
          }
        }
      }
    });
  }
}

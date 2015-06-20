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
package org.apache.reef.io.network.naming;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.cache.Cache;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * NameClient looking up local name server
 */
public final class NameClientLocalImpl implements NameClient {

  private static final Logger LOG = Logger.getLogger(NameClientLocalImpl.class.getName());
  private final NameServer nameServer;
  private final Cache<Identifier, InetSocketAddress> cache;
  private final int retryCount;
  private final int retryTimeout;

  @Inject
  private NameClientLocalImpl(
      final NameServer nameServer,
      final @Parameter(NameLookupClient.CacheTimeout.class) long timeout,
      final @Parameter(NameLookupClient.RetryCount.class) int retryCount,
      final @Parameter(NameLookupClient.RetryTimeout.class) int retryTimeout) {
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
        final int origRetryCount = NameClientLocalImpl.this.retryCount;
        int retryCount = origRetryCount;
        while (true) {
          try {
            InetSocketAddress addr =  nameServer.lookup(id);
            if (addr == null) {
              throw new NullPointerException();
            } else {
              return addr;
            }
          } catch (final NullPointerException e) {
            if (retryCount <= 0) {
              throw e;
            } else {
              final int retryTimeout = NameClientLocalImpl.this.retryTimeout
                  * (origRetryCount - retryCount + 1);
              LOG.log(Level.WARNING,
                  "Caught Naming Exception while looking up " + id
                      + " with Name Server. Will retry " + retryCount
                      + " time(s) after waiting for " + retryTimeout + " msec.");
              Thread.sleep(retryTimeout * retryCount);
              --retryCount;
            }
          }
        }
      }
    });
  }
}

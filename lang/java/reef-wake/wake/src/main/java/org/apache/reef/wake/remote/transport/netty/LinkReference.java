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
package org.apache.reef.wake.remote.transport.netty;

import org.apache.reef.wake.remote.transport.Link;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A reference for a link.
 * When channel became active, LinkReference is created and mapped with remote address.
 */
public final class LinkReference {

  private final AtomicInteger connectInProgress = new AtomicInteger(0);
  private Link<?> link;

  public LinkReference() {
  }

  public LinkReference(final Link<?> link) {
    this.link = link;
  }

  public synchronized Link<?> getLink() {
    return this.link;
  }

  public synchronized void setLink(final Link<?> link) {
    this.link = link;
  }

  public AtomicInteger getConnectInProgress() {
    return this.connectInProgress;
  }
}

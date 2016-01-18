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

import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class NameServiceCloseHandler implements EventHandler<ContextStop> {

  private static final Logger LOG = Logger.getLogger(NameServiceCloseHandler.class.getName());

  private final AutoCloseable toClose;

  @Inject
  public NameServiceCloseHandler(final NameServer toClose) {
    this.toClose = toClose;
  }

  @Override
  public void onNext(final ContextStop event) {
    try {
      LOG.log(Level.FINEST, "Closing {0}", this.toClose);
      this.toClose.close();
    } catch (final Exception ex) {
      LOG.log(Level.SEVERE, "Exception while closing " + this.toClose, ex);
    }
  }
}

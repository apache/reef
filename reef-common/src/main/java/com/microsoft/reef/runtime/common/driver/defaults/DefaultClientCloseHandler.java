/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.reef.runtime.common.driver.defaults;

import com.microsoft.wake.EventHandler;
import org.apache.reef.util.ThreadLogger;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default handler for close messages from the client: Throw an Exception.
 */
public final class DefaultClientCloseHandler implements EventHandler<Void> {

  private static final Logger LOG = Logger.getLogger(DefaultClientCloseHandler.class.getName());

  @Inject
  DefaultClientCloseHandler() {
  }

  @Override
  public void onNext(final Void aVoid) {
    final String message = ThreadLogger.getFormattedThreadList(
        "Received a close message from the client, but no handler was bound for it. Active threads: ");
    LOG.log(Level.WARNING, message);
    throw new RuntimeException(message);
  }
}

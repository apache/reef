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

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default handler for ActiveContext from previous evaluator during driver restart: Close it.
 */
public final class DefaultDriverRestartContextActiveHandler implements EventHandler<ActiveContext> {

  private static final Logger LOG = Logger.getLogger(DefaultDriverRestartContextActiveHandler.class.getName());

  @Inject
  private DefaultDriverRestartContextActiveHandler() {
  }

  @Override
  public void onNext(final ActiveContext activeContext) {
    LOG.log(Level.INFO, "Received ActiveContext running on previous Evaluator during driver restart: {0} :: CLOSING", activeContext);
    activeContext.close();
  }
}

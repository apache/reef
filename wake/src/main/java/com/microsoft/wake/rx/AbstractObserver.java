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
package com.microsoft.wake.rx;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An {@link Observer} with logging-only onError and onCompleted() methods.
 * 
 * @param <T> type
 */
public abstract class AbstractObserver<T> implements Observer<T> {

  private static final Logger LOG = Logger.getLogger(AbstractObserver.class.getName());

  @Override
  public void onError(final Exception error) {
    LOG.log(Level.INFO, "The observer " + this.getClass().toString() + "has received an Exception: " + error);
  }

  @Override
  public void onCompleted() {
    LOG.log(Level.FINEST, "The observer " + this.getClass().toString() + "has received an onCompleted() ");
  }

}

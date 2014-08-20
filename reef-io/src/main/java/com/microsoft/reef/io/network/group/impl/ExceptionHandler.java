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
package com.microsoft.reef.io.network.group.impl;

import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * ExceptionHandler registered with {@link NetworkService}
 */
public class ExceptionHandler implements EventHandler<Exception> {
  /**
   * Standard Java logger object.
   */
  private static final Logger logger = Logger.getLogger(ExceptionHandler.class.getName());

  @Inject
  public ExceptionHandler() {
    //intentionally blank
  }

  @Override
  public void onNext(Exception e) {
    logger
        .severe("Exception occurred while processing a GroupComm operation caused by "
            + e.getCause());
  }

}

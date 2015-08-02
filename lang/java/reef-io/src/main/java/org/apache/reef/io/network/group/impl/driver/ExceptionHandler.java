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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 */
public class ExceptionHandler implements EventHandler<Exception> {
  private static final Logger LOG = Logger.getLogger(ExceptionHandler.class.getName());
  private List<Exception> exceptions = new ArrayList<>();

  @Inject
  public ExceptionHandler() {
  }

  @Override
  public synchronized void onNext(final Exception ex) {
    LOG.entering("ExceptionHandler", "onNext", new Object[]{ex});
    exceptions.add(ex);
    LOG.finest("Got an exception. Added it to list(" + exceptions.size() + ")");
    LOG.exiting("ExceptionHandler", "onNext");
  }

  public synchronized boolean hasExceptions() {
    LOG.entering("ExceptionHandler", "hasExceptions");
    final boolean ret = !exceptions.isEmpty();
    LOG.finest("There are " + exceptions.size() + " exceptions. Clearing now");
    exceptions.clear();
    LOG.exiting("ExceptionHandler", "hasExceptions", ret);
    return ret;
  }

}

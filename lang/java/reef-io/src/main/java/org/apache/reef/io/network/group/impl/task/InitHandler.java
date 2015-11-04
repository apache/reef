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
package org.apache.reef.io.network.group.impl.task;

import org.apache.reef.io.network.exception.ParentDeadException;
import org.apache.reef.io.network.group.api.operators.GroupCommOperator;
import org.apache.reef.wake.EventHandler;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

class InitHandler implements EventHandler<GroupCommOperator> {

  private static final Logger LOG = Logger.getLogger(InitHandler.class.getName());

  private ParentDeadException exception = null;
  private final CountDownLatch initLatch;

  InitHandler(final CountDownLatch initLatch) {
    this.initLatch = initLatch;
  }

  @Override
  public void onNext(final GroupCommOperator op) {
    LOG.entering("InitHandler", "onNext", op);
    try {
      op.initialize();
    } catch (final ParentDeadException e) {
      this.exception = e;
    }
    initLatch.countDown();
    LOG.exiting("InitHandler", "onNext", op);
  }

  public ParentDeadException getException() {
    return exception;
  }
}

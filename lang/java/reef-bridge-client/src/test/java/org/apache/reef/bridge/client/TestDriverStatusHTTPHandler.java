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
package org.apache.reef.bridge.client;

import org.apache.reef.proto.ReefServiceProtos;
import org.junit.Assert;
import org.junit.Test;

import java.util.logging.Logger;

/**
 * Tests for {@link DriverStatusHTTPHandler}.
 */
public final class TestDriverStatusHTTPHandler {

  private static final Logger LOG = Logger.getLogger(TestDriverStatusHTTPHandler.class.getName());
  private static final String TEST_DRIVER_ID = "TestDriver";

  /**
   * An array of all statuses to test.
   */
  private final ReefServiceProtos.JobStatusProto[] allStatuses = new ReefServiceProtos.JobStatusProto[] {
      ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(TEST_DRIVER_ID)
          .setState(ReefServiceProtos.State.INIT).build(),
      ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(TEST_DRIVER_ID)
          .setState(ReefServiceProtos.State.RUNNING).build(),
      ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(TEST_DRIVER_ID)
          .setState(ReefServiceProtos.State.DONE).build(),
      ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(TEST_DRIVER_ID)
          .setState(ReefServiceProtos.State.SUSPEND).build(),
      ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(TEST_DRIVER_ID)
          .setState(ReefServiceProtos.State.FAILED).build(),
      ReefServiceProtos.JobStatusProto.newBuilder()
          .setIdentifier(TEST_DRIVER_ID)
          .setState(ReefServiceProtos.State.KILLED).build()
  };

  /**
   * Make sure we get the right strings for the driver status.
   */
  @Test
  public void testMessageForProto() {
    for (final ReefServiceProtos.JobStatusProto status : allStatuses) {
      Assert.assertEquals(status.getState().name(), DriverStatusHTTPHandler.getMessageForStatus(status));
    }
  }

  /**
   * Make sure {@link DriverStatusHTTPHandler} implements
   * {@link org.apache.reef.runtime.common.driver.client.JobStatusHandler}.
   */
  @Test
  public void testLastStatus() {
    final DriverStatusHTTPHandler tester = new DriverStatusHTTPHandler();

    for (final ReefServiceProtos.JobStatusProto status : allStatuses) {
      tester.onNext(status);
      Assert.assertSame(status, tester.getLastStatus());
    }
  }

  /**
   * Test the wait and notify for correctness.
   */
  @Test
  public void testAsyncCalls() throws InterruptedException {
    final DriverStatusHTTPHandler tester = new DriverStatusHTTPHandler();

    final WaitingRunnable waiter = new WaitingRunnable(tester);

    for (final ReefServiceProtos.JobStatusProto status : allStatuses) {
      final Thread waitingThread = new Thread(waiter);
      waitingThread.start();
      Assert.assertTrue(waitingThread.isAlive());
      Assert.assertNull(waiter.getResult());
      tester.onNext(status);
      waitingThread.join();
      Assert.assertEquals(DriverStatusHTTPHandler.getMessageForStatus(status), waiter.getResult());
    }
  }

  private final class WaitingRunnable implements Runnable {
    private final DriverStatusHTTPHandler handler;
    private String result = null;

    private WaitingRunnable(final DriverStatusHTTPHandler handler) {
      this.handler = handler;
    }

    @Override
    public synchronized void run() {
      result = handler.waitAndGetMessage();
    }

    public synchronized String getResult() {
      final String returnValue = result;
      result = null;
      return returnValue;
    }
  }
}

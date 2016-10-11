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
package org.apache.reef.tests;

import org.apache.reef.client.LauncherStatus;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.junit.Assert;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utilities used in tests.
 */
public final class TestUtils {

  private static final Logger LOG = Logger.getLogger(TestUtils.class.getName());

  /**
   * Make sure the launcher status is FAILED and it has the specified exception in the stack.
   *
   * @param status launcher status. Must be FAILED for test to pass.
   * @param clazz  resourcemanager exception that should be in the stack of exceptions of the launcher status.
   */
  public static void assertLauncherFailure(
      final LauncherStatus status, final Class<? extends Throwable> clazz) {
    Assert.assertEquals(LauncherStatus.FAILED, status);
    final Throwable ex = status.getError().orElse(null);
    if (!hasCause(ex, clazz)) {
      LOG.log(Level.WARNING, "Unexpected Error: " + status, status.getError().get());
      Assert.fail("Unexpected error: " + status.getError().orElse(null));
    }
  }

  /**
   * Make sure the job status is FAILED and it has the specified exception in the stack.
   *
   * @param status Job status. State must be FAILED for test to pass.
   * @param clazz  Exception that should be in the stack of exceptions of the launcher status.
   */
  public static void assertJobFailure(
      final ReefServiceProtos.JobStatusProto status,
      final ExceptionCodec exceptionCodec,
      final Class<? extends Throwable> clazz) {

    Assert.assertNotNull("Final job status must not be null", status);

    Assert.assertTrue("Job state missing", status.hasState());
    Assert.assertEquals("Unexpected final job state", ReefServiceProtos.State.FAILED, status.getState());

    Assert.assertTrue("Job status must contain an exception", status.hasException());

    final Throwable ex = exceptionCodec.fromBytes(status.getException().toByteArray()).orElse(null);
    Assert.assertNotNull("Unable to decode exception", ex);

    if (!hasCause(ex, clazz)) {
      LOG.log(Level.WARNING, "Unexpected Error: " + status, ex);
      Assert.fail("Unexpected error: " + ex);
    }
  }

  /**
   * Return True if cause chain of exception ex contains
   * exception of class clazz (or one inherited from it).
   *
   * @param ex exception to analyze (can be null).
   * @param clazz class inherited from type Throwable.
   * @return True if ex or any other exception in its cause chain is instance of class clazz.
   */
  public static boolean hasCause(final Throwable ex, final Class<? extends Throwable> clazz) {
    for (Throwable exception = ex; exception != null; exception = exception.getCause()) {
      if (clazz.isInstance(exception)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private TestUtils() {
  }
}

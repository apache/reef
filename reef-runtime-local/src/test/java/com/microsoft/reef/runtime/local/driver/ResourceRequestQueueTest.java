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
package com.microsoft.reef.runtime.local.driver;

import com.microsoft.reef.proto.DriverRuntimeProtocol;
import org.junit.Assert;
import org.junit.Test;

public class ResourceRequestQueueTest {

  @Test
  public void testEmptyAfterConstruction() {
    final ResourceRequestQueue q = new ResourceRequestQueue();
    Assert.assertFalse("A freshly generated queue should be empty", q.hasOutStandingRequests());
  }

  @Test
  public void testNotEmptyAfterInsert() {
    final ResourceRequestQueue q = new ResourceRequestQueue();
    q.add(getAlmostSatisfied());
    Assert.assertTrue("A queue should not be empty after an insert.", q.hasOutStandingRequests());
  }

  @Test
  public void testSatisfaction() {
    final ResourceRequestQueue q = new ResourceRequestQueue();
    for (int i = 0; i < 1; ++i) {
      q.add(getAlmostSatisfied());
      Assert.assertTrue("A queue should not be empty after an insert.", q.hasOutStandingRequests());
      q.satisfyOne();
      Assert.assertFalse("The queue should be empty after all requests have been satisfied", q.hasOutStandingRequests());
    }

    final int nInsert = 10;
    for (int i = 0; i < nInsert; ++i) {
      q.add(getAlmostSatisfied());
    }
    for (int i = 0; i < nInsert; ++i) {
      q.satisfyOne();
    }
    Assert.assertFalse("The queue should be empty after all requests have been satisfied", q.hasOutStandingRequests());
  }

  private ResourceRequest getAlmostSatisfied() {
    return new ResourceRequest(DriverRuntimeProtocol.ResourceRequestProto.newBuilder().setResourceCount(1).setMemorySize(128).build());
  }
}
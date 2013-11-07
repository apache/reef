/**
 * Copyright (C) 2013 Microsoft Corporation
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
import com.microsoft.reef.proto.ReefServiceProtos;
import org.junit.Assert;
import org.junit.Test;

public final class ResourceRequestTest {

  @Test
  public void testInitializer() {
    final ResourceRequest rr = get(1);
    Assert.assertFalse("A fresh request should not be satisfied.", rr.isSatisfied());
    try {
      final ResourceRequest rr2 = new ResourceRequest(null);
      Assert.fail("Resource Requests should throw an IllegalArgumentException when initialized with null");
    } catch (IllegalArgumentException ex) {
    }
  }

  @Test
  public void testSatisfaction() {
    final int n = 10;
    final ResourceRequest rr = get(n);
    for (int i = 0; i < n; ++i) {
      rr.satisfyOne();
    }
    Assert.assertTrue("A satisfied request should tell so", rr.isSatisfied());
  }

  @Test
  public void testOverSatisfaction() {
    final ResourceRequest rr = get(1);
    rr.satisfyOne();
    try {
      rr.satisfyOne();
      Assert.fail("Satisfying more than the request should throw an IllegalStateException");
    } catch (IllegalStateException ex) {
    }
  }

  private ResourceRequest get(final int n) {
    return new ResourceRequest(DriverRuntimeProtocol.ResourceRequestProto.newBuilder().setResourceCount(n).setResourceSize(ReefServiceProtos.SIZE.SMALL).build());
  }
}
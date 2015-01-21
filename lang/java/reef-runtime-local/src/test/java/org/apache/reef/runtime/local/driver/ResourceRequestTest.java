/**
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
package org.apache.reef.runtime.local.driver;

import org.apache.reef.proto.DriverRuntimeProtocol;
import org.junit.Assert;
import org.junit.Test;

public final class ResourceRequestTest {

  @Test()
  public void testInitializer() {
    final ResourceRequest rr = get(1);
    Assert.assertFalse("A fresh request should not be satisfied.", rr.isSatisfied());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInitializationWithNull() {
    final ResourceRequest rr2 = new ResourceRequest(null);
    Assert.fail("Passing null to the ResourceRequest constructor should throw an IllegalArgumentException.");
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

  @Test(expected = IllegalStateException.class)
  public void testOverSatisfaction() {
    final ResourceRequest rr = get(1);
    rr.satisfyOne();
    rr.satisfyOne();
    Assert.fail("Satisfying more than the request should throw an IllegalStateException");
  }

  private ResourceRequest get(final int n) {
    return new ResourceRequest(DriverRuntimeProtocol.ResourceRequestProto.newBuilder().setResourceCount(n).setMemorySize(128).build());
  }
}
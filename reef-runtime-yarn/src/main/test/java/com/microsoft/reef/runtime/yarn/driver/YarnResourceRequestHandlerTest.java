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
package com.microsoft.reef.runtime.yarn.driver;

import com.microsoft.reef.driver.catalog.ResourceCatalog;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.runtime.common.driver.EvaluatorRequestorImpl;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for YarnResourceRequestHandler.
 */
public final class YarnResourceRequestHandlerTest {
  private final ApplicationMasterRegistration applicationMasterRegistration = new ApplicationMasterRegistration();
  private final MockContainerRequestHandler containerRequestHandler = new MockContainerRequestHandler();
  private final YarnResourceRequestHandler resourceRequestHandler = new YarnResourceRequestHandler(containerRequestHandler, applicationMasterRegistration);
  private final ResourceCatalog resourceCatalog = Mockito.mock(ResourceCatalog.class);
  private final EvaluatorRequestor evaluatorRequestor = new EvaluatorRequestorImpl(resourceCatalog, resourceRequestHandler);

  private class MockContainerRequestHandler implements YarnContainerRequestHandler {
    private AMRMClient.ContainerRequest[] requests;

    @Override
    public void onContainerRequest(AMRMClient.ContainerRequest... containerRequests) {
      this.requests = containerRequests;
    }

    public AMRMClient.ContainerRequest[] getRequests() {
      return requests;
    }
  }

  /**
   * Tests whether the amount of memory is transferred correctly.
   */
  @Test
  public void testDifferentMemory() {
    final EvaluatorRequest requestOne = EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(64)
        .setNumberOfCores(1)
        .build();
    final EvaluatorRequest requestTwo = EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(128)
        .setNumberOfCores(2)
        .build();
    {
      evaluatorRequestor.submit(requestOne);
      Assert.assertEquals("Request in REEF and YARN form should have the same amount of memory",
          requestOne.getMegaBytes(),
          containerRequestHandler.getRequests()[0].getCapability().getMemory()
      );
    }
    {
      evaluatorRequestor.submit(requestTwo);
      Assert.assertEquals("Request in REEF and YARN form should have the same amount of memory",
          requestTwo.getMegaBytes(),
          containerRequestHandler.getRequests()[0].getCapability().getMemory()
      );
    }
    {
      evaluatorRequestor.submit(requestOne);
      Assert.assertNotEquals("Request in REEF and YARN form should have the same amount of memory",
          requestTwo.getMegaBytes(),
          containerRequestHandler.getRequests()[0].getCapability().getMemory()
      );
    }
  }

  @Test
  public void testEvaluatorCount() {
    final EvaluatorRequest requestOne = EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(64)
        .setNumberOfCores(1)
        .build();
    final EvaluatorRequest requestTwo = EvaluatorRequest.newBuilder()
        .setNumber(2)
        .setMemory(128)
        .setNumberOfCores(2)
        .build();
    {
      evaluatorRequestor.submit(requestOne);
      Assert.assertEquals("Request in REEF and YARN form should have the same number of Evaluators",
          requestOne.getNumber(),
          containerRequestHandler.getRequests().length
      );
    }
        {
      evaluatorRequestor.submit(requestTwo);
      Assert.assertEquals("Request in REEF and YARN form should have the same number of Evaluators",
          requestTwo.getNumber(),
          containerRequestHandler.getRequests().length
      );
    }
    {
      evaluatorRequestor.submit(requestTwo);
      Assert.assertNotEquals("Request in REEF and YARN form should have the same number of Evaluators",
          requestOne.getNumber(),
          containerRequestHandler.getRequests().length
      );
    }
  }


}

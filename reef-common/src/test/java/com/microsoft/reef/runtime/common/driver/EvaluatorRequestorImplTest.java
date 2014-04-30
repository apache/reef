package com.microsoft.reef.runtime.common.driver;

import com.microsoft.reef.driver.catalog.ResourceCatalog;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.api.ResourceRequestHandler;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 * Tests for EvaluatorRequestorImpl.
 */
public class EvaluatorRequestorImplTest {
  private final ResourceCatalog resourceCatalog = mock(ResourceCatalog.class);

  private class DummyRequestHandler implements ResourceRequestHandler {
    private DriverRuntimeProtocol.ResourceRequestProto request;

    @Override
    public void onNext(DriverRuntimeProtocol.ResourceRequestProto resourceRequestProto) {
      this.request = resourceRequestProto;
    }

    public DriverRuntimeProtocol.ResourceRequestProto get() {
      return this.request;
    }
  }

  /**
   * If only memory, no count is given, 1 evaluator should be requested.
   */
  @Test
  public void testMemoryOnly() {
    final int memory = 777;
    final DummyRequestHandler requestHandler = new DummyRequestHandler();
    final EvaluatorRequestor evaluatorRequestor = new EvaluatorRequestorImpl(resourceCatalog, requestHandler);
    evaluatorRequestor.submit(EvaluatorRequest.newBuilder().setMemory(memory).build());
    Assert.assertEquals("Memory request did not make it", requestHandler.get().getMemorySize(), memory);
    Assert.assertEquals("Number of requests did not make it", requestHandler.get().getResourceCount(), 1);
  }

  /**
   * Checks whether memory and count make it correctly.
   */
  @Test
  public void testMemoryAndCount() {
    final int memory = 777;
    final int count = 9;
    final DummyRequestHandler requestHandler = new DummyRequestHandler();
    final EvaluatorRequestor evaluatorRequestor = new EvaluatorRequestorImpl(resourceCatalog, requestHandler);
    evaluatorRequestor.submit(EvaluatorRequest.newBuilder().setMemory(memory).setNumber(count).build());
    Assert.assertEquals("Memory request did not make it", requestHandler.get().getMemorySize(), memory);
    Assert.assertEquals("Number of requests did not make it", requestHandler.get().getResourceCount(), count);
  }

  /**
   * Expect an IllegalArgumentException when a non-positive memory amount is passed.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testIllegalMemory() {
    final int memory = 0;
    final int count = 1;
    final DummyRequestHandler requestHandler = new DummyRequestHandler();
    final EvaluatorRequestor evaluatorRequestor = new EvaluatorRequestorImpl(resourceCatalog, requestHandler);
    evaluatorRequestor.submit(EvaluatorRequest.newBuilder().setMemory(memory).setNumber(count).build());
  }

  /**
   * Expect an IllegalArgumentException when a non-positive evaluator count is passed.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testIllegalCount() {
    final int memory = 128;
    final int count = 0;
    final DummyRequestHandler requestHandler = new DummyRequestHandler();
    final EvaluatorRequestor evaluatorRequestor = new EvaluatorRequestorImpl(resourceCatalog, requestHandler);
    evaluatorRequestor.submit(EvaluatorRequest.newBuilder().setMemory(memory).setNumber(count).build());
  }
}

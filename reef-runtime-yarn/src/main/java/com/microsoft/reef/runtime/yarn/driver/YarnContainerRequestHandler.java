package com.microsoft.reef.runtime.yarn.driver;

import com.microsoft.tang.annotations.DefaultImplementation;
import org.apache.hadoop.yarn.client.api.AMRMClient;

@DefaultImplementation(YarnContainerRequestHandlerImpl.class)
public interface YarnContainerRequestHandler {
  /**
   * Enqueue a set of container requests with YARN.
   *
   * @param containerRequests
   */
  void onContainerRequest(final AMRMClient.ContainerRequest... containerRequests);
}

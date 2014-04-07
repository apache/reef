package com.microsoft.reef.runtime.common.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.client.CompletedJob;

/**
 * An implementation of CompletedJob
 */
@ClientSide
@Private
final class CompletedJobImpl implements CompletedJob {
  private final String jobId;

  CompletedJobImpl(final String jobId) {
    this.jobId = jobId;
  }

  @Override
  public String getId() {
    return this.jobId;
  }

  @Override
  public String toString() {
    return "CompletedJob{'" + jobId + "'}";
  }
}

package com.microsoft.reef.runtime.hdinsight.client.sslhacks;

import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

/**
 * Default Client Provider with default SSL checks.
 */
public final class DefaultClientProvider implements ClientProvider {
  @Inject
  DefaultClientProvider() {
  }

  @Override
  public Client getNewClient() {
    return ClientBuilder.newBuilder().build();
  }
}

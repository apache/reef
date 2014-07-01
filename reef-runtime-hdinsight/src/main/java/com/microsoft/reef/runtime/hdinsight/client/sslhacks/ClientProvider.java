package com.microsoft.reef.runtime.hdinsight.client.sslhacks;

import javax.ws.rs.client.Client;

/**
 * Provides instances of javax.ws.rs.client.Client
 */
public interface ClientProvider {

  /**
   * @return a new Client
   */
  public Client getNewClient();
}

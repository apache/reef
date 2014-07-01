package com.microsoft.reef.runtime.hdinsight.client.sslhacks;

import javax.inject.Inject;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of ClientProvider that produces Clients that do not check SSL.
 */
public final class TrustingClientProvider implements ClientProvider {
  private static final Logger LOG = Logger.getLogger(TrustingClientProvider.class.getName());

  @Inject
  public TrustingClientProvider() {
  }

  @Override
  public Client getNewClient() {
    LOG.log(Level.SEVERE, "DANGER: INSTANTIATING HTTP CLIENT WITH NO SSL CHECKS.");
    try {
      final SSLContext sslContext = this.getSSLContext();
      HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
      return ClientBuilder.newBuilder()
          .sslContext(sslContext)
          .hostnameVerifier(new IgnoringHostnameVerifier())
          .build();
    } catch (final KeyManagementException | NoSuchAlgorithmException ex) {
      LOG.log(Level.SEVERE, "SSL context error. Cannot instantiate HTTP client", ex);
      throw new RuntimeException("Unable to instantiate HTTP Client", ex);
    }
  }

  private SSLContext getSSLContext() throws KeyManagementException, NoSuchAlgorithmException {
    final SSLContext sc = SSLContext.getInstance("TLS");
    sc.init(new KeyManager[0], new TrustManager[]{new TrustingTrustManager()}, new SecureRandom());
    return sc;
  }
}

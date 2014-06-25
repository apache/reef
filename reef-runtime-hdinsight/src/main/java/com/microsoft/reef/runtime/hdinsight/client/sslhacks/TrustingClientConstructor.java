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
package com.microsoft.reef.runtime.hdinsight.client.sslhacks;

import com.microsoft.tang.ExternalConstructor;

import javax.inject.Inject;
import javax.net.ssl.HttpsURLConnection;
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
 * ExternalConstructor for the Client of the javax.ws API
 */
public final class TrustingClientConstructor implements ExternalConstructor<Client> {

  private static final Logger LOG = Logger.getLogger(TrustingClientConstructor.class.getName());

  @Inject
  TrustingClientConstructor() {
    LOG.log(Level.SEVERE, "DANGER: INSTANTIATING HTTP CLIENTS WITH NO SSL CHECKS.");
  }

  @Override
  public Client newInstance() {
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
    sc.init(null, new TrustManager[]{new TrustingTrustManager()}, new SecureRandom());
    return sc;
  }
}

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
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.BasicClientConnectionManager;

import javax.inject.Inject;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Client constructor that produces Clients that do not check SSL.
 */
public final class UnsafeClientConstructor implements ExternalConstructor<CloseableHttpClient> {

  @Inject
  UnsafeClientConstructor() {
    Logger.getLogger(UnsafeClientConstructor.class.getName())
        .log(Level.SEVERE, "DANGER: INSTANTIATING HTTP CLIENT WITH NO SSL CHECKS.");
  }

  @Override
  public CloseableHttpClient newInstance() {
    try {
      final SSLSocketFactory socketFactory = new SSLSocketFactory(this.getSSLContext());
      socketFactory.setHostnameVerifier(new UnsafeHostNameVerifier());
      final SchemeRegistry schemeRegistry = new SchemeRegistry();
      schemeRegistry.register(new Scheme("https", 443, socketFactory));
      final ClientConnectionManager clientConnectionManager = new BasicClientConnectionManager(schemeRegistry);
      return new DefaultHttpClient(clientConnectionManager);
    } catch (final KeyManagementException | NoSuchAlgorithmException ex) {
      throw new RuntimeException("Unable to instantiate HTTP Client", ex);
    }
  }

  private SSLContext getSSLContext() throws KeyManagementException, NoSuchAlgorithmException {
    final SSLContext sc = SSLContext.getInstance("TLS");
    sc.init(new KeyManager[0], new TrustManager[]{new UnsafeTrustManager()}, new SecureRandom());
    return sc;
  }


}

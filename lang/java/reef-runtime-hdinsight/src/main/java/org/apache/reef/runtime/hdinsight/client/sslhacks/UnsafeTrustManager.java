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
package org.apache.reef.runtime.hdinsight.client.sslhacks;

import javax.inject.Inject;
import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * A TrustManager that trusts all certificates. Basically the "GOTO FAIL" bug implemented in Java.
 * <p/>
 * Hence: DO NOT USE THIS CLASS UNLESS DEBUGGING.
 */
final class UnsafeTrustManager implements X509TrustManager {
  @Inject
  UnsafeTrustManager() {
  }

  @Override
  public void checkClientTrusted(final X509Certificate[] x509Certificates, final String s) throws CertificateException {
  }

  @Override
  public void checkServerTrusted(final X509Certificate[] x509Certificates, final String s) throws CertificateException {
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return new X509Certificate[0];
  }

}

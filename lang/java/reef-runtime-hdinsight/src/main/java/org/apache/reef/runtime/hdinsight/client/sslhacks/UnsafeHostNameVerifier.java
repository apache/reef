/*
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

import org.apache.http.conn.ssl.X509HostnameVerifier;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.security.cert.X509Certificate;

final class UnsafeHostNameVerifier implements X509HostnameVerifier {

  @Override
  public void verify(final String host, final SSLSocket ssl) throws IOException {

  }

  @Override
  public void verify(final String host, final X509Certificate cert) throws SSLException {

  }

  @Override
  public void verify(final String host, final String[] cns, final String[] subjectAlts) throws SSLException {

  }

  @Override
  public boolean verify(final String s, final SSLSession sslSession) {
    return true;
  }
}

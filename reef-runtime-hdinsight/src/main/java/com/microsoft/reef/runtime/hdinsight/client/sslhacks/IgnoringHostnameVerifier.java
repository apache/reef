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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import java.util.logging.Level;
import java.util.logging.Logger;

final class IgnoringHostnameVerifier implements HostnameVerifier {
  private static final Logger LOG = Logger.getLogger(IgnoringHostnameVerifier.class.getName());

  @Override
  public boolean verify(final String hostName, final SSLSession sslSession) {
    LOG.log(Level.INFO, "Ignoring verfication of: {0}", hostName);
    return true;
  }
}

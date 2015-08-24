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
package org.apache.reef.runtime.yarn.client;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads security token from user credentials.
 */
final class UserCredentialSecurityTokenProvider implements SecurityTokenProvider {

  private static final Logger LOG = Logger.getLogger(UserCredentialSecurityTokenProvider.class.getName());

  @Inject
  private UserCredentialSecurityTokenProvider(){}

  @Override
  public byte[] getTokens() {
    try {
      final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      final Credentials credentials = ugi.getCredentials();
      if (credentials.numberOfTokens() > 0) {
        try(final DataOutputBuffer dob = new DataOutputBuffer()) {
          credentials.writeTokenStorageToStream(dob);
          return dob.getData();
        }
      }
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Could not access tokens in user credentials.", e);
    }

    LOG.log(Level.FINE, "No security token found.");
    return null;
  }
}

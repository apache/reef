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
package org.apache.reef.runtime.yarn.client.unmanaged;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.UserCredentials;

import javax.inject.Inject;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A holder class for the proxy UserGroupInformation object
 * required for Unmanaged YARN Application Master in REEF-on-REEF or REEF-on-Spark mode.
 */
@Private
@ClientSide
@DriverSide
public final class YarnProxyUser implements UserCredentials {

  private static final Logger LOG = Logger.getLogger(YarnProxyUser.class.getName());

  private UserGroupInformation proxyUGI = null;

  @Inject
  private YarnProxyUser() { }

  /**
   * Get the YARN proxy user information. If not set, return the (global) current user.
   * @return Proxy user group information, if set; otherwise, return current YARN user.
   * @throws IOException if proxy user is not set AND unable to obtain current YARN user information.
   */
  public UserGroupInformation get() throws IOException {

    final UserGroupInformation effectiveUGI =
        this.proxyUGI == null ? UserGroupInformation.getCurrentUser() : this.proxyUGI;

    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(Level.FINEST, "UGI: get: {0}", ugiToString("EFFECTIVE", effectiveUGI));
    }

    return effectiveUGI;
  }

  /**
   * Check if the proxy user is set.
   * @return true if proxy user set, false otherwise.
   */
  @Override
  public boolean isSet() {
    return this.proxyUGI != null;
  }

  /**
   * Set YARN user. This method can be called only once per class instance.
   * @param name Name of the new proxy user.
   * @param hostUser User credentials to copy. Must be an instance of YarnProxyUser.
   */
  @Override
  public void set(final String name, final UserCredentials hostUser) throws IOException {

    assert this.proxyUGI == null;
    assert hostUser instanceof YarnProxyUser;

    LOG.log(Level.FINE, "UGI: user {0} copy from: {1}", new Object[] {name, hostUser});

    final UserGroupInformation hostUGI = ((YarnProxyUser) hostUser).get();
    final Collection<Token<? extends TokenIdentifier>> tokens = hostUGI.getCredentials().getAllTokens();

    this.set(name, hostUGI, tokens.toArray(new Token[tokens.size()]));
  }

  /**
   * Create YARN proxy user and add security tokens to its credentials.
   * This method can be called only once per class instance.
   * @param proxyName Name of the new proxy user.
   * @param hostUGI YARN user to impersonate the proxy.
   * @param tokens Security tokens to add to the new proxy user's credentials.
   */
  @SafeVarargs
  public final void set(final String proxyName,
      final UserGroupInformation hostUGI, final Token<? extends TokenIdentifier>... tokens) {

    assert this.proxyUGI == null;
    this.proxyUGI = UserGroupInformation.createProxyUser(proxyName, hostUGI);

    for (final Token<? extends TokenIdentifier> token : tokens) {
      this.proxyUGI.addToken(token);
    }

    LOG.log(Level.FINE, "UGI: user {0} set to: {1}", new Object[] {proxyName, this});
  }

  /**
   * Execute the privileged action as a given user.
   * If user credentials are not set, execute the action outside the user context.
   * @param action an action to run.
   * @param <T> action return type.
   * @return result of an action.
   * @throws Exception whatever the action can throw.
   */
  public <T> T doAs(final PrivilegedExceptionAction<T> action) throws Exception {
    LOG.log(Level.FINE, "{0} execute {1}", new Object[] {this, action});
    return this.proxyUGI == null ? action.run() : this.proxyUGI.doAs(action);
  }

  @Override
  public String toString() {
    return this.proxyUGI == null ? "UGI: { CURRENT user: null }" : ugiToString("PROXY", this.proxyUGI);
  }

  private static String ugiToString(final String prefix, final UserGroupInformation ugi) {
    return String.format("UGI: { %s user: %s tokens: %s }", prefix, ugi, ugi.getCredentials().getAllTokens());
  }
}

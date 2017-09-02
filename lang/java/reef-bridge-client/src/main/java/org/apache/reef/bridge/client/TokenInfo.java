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
package org.apache.reef.bridge.client;

/**
 * Object that holds token data.
 */
final class TokenInfo {
  private final String kind;
  private final String service;
  private final int tokenLen;
  private byte[] password;
  private byte[] tokenKey;

  /**
   * Construct a new TokenInfo object.
   * @param kind
   * @param service
   * @param tokenLen
   */
  TokenInfo(final String kind, final String service, final int tokenLen) {
    this.kind = kind;
    this.service = service;
    this.tokenLen = tokenLen;
  }

  /**
   * Set password for the token.
   * @param pw
   */
  void setPassword(final byte[] pw) {
    this.password = pw;
  }

  /**
   * Set token key.
   * @param key
   */
  void setKey(final byte[] key) {
    this.tokenKey = key;
  }

  /**
   * Get length of the token key.
   * @return
   */
  int getLength() {
    return tokenLen;
  }

  /**
   * Get password of the token.
   * @return
   */
  byte[] getPassword() {
    return password;
  }

  /**
   * Get kind of the token.
   * @return
   */
  String getKind() {
    return kind;
  }

  /**
   * Get service of the token.
   * @return
   */
  String getService() {
    return service;
  }

  /**
   * Get token key.
   * @return
   */
  byte[] getKey() {
    return tokenKey;
  }
}

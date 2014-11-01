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
package org.apache.reef.javabridge;

public final class HttpServerEventBridge extends NativeBridge {
  private String queryString;
  private byte[] queryRequestData;
  private byte[] queryResponseData;
  private String queryResult;
  private String uriSpecification;

  public HttpServerEventBridge(final String queryStr) {
    this.queryString = queryStr;
  }

  public HttpServerEventBridge(final byte[] queryRequestData) {
    this.queryRequestData = queryRequestData;
  }

  public final String getQueryString() {
    return queryString;
  }

  public final void setQueryString(final String queryStr) {
    this.queryString = queryStr;
  }

  public final String getQueryResult() {
    return queryResult;
  }

  public final void setQueryResult(final String queryResult) {
    this.queryResult = queryResult;
  }

  public final String getUriSpecification() {
    return uriSpecification;
  }

  public final void setUriSpecification(final String uriSpecification) {
    this.uriSpecification = uriSpecification;
  }

  public final byte[] getQueryRequestData() {
    return queryRequestData;
  }

  public final void setQueryRequestData(final byte[] queryRequestData) {
    this.queryRequestData = queryRequestData;
  }

  public final byte[] getQueryResponseData() {
    return queryResponseData;
  }

  public final void setQueryResponseData(final byte[] responseData) {
    queryResponseData = responseData;
  }

  @Override
  public void close() {
  }
}

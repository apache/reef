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
package org.apache.reef.javabridge;

import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;

/**
 * The Java-CLR bridge object for http server events.
 */
@Private
@Interop(
    CppFiles = { "Clr2JavaImpl.h",  "HttpServerClr2Java.cpp"},
    CsFiles = { "IHttpServerBridgeClr2Java.cs", "HttpMessage.cs" })
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

  public String getQueryString() {
    return queryString;
  }

  public void setQueryString(final String queryStr) {
    this.queryString = queryStr;
  }

  public String getQueryResult() {
    return queryResult;
  }

  public void setQueryResult(final String queryResult) {
    this.queryResult = queryResult;
  }

  public String getUriSpecification() {
    return uriSpecification;
  }

  public void setUriSpecification(final String uriSpecification) {
    this.uriSpecification = uriSpecification;
  }

  public byte[] getQueryRequestData() {
    return queryRequestData;
  }

  public void setQueryRequestData(final byte[] queryRequestData) {
    this.queryRequestData = queryRequestData;
  }

  public byte[] getQueryResponseData() {
    return queryResponseData;
  }

  public void setQueryResponseData(final byte[] responseData) {
    queryResponseData = responseData;
  }

  @Override
  public void close() {
  }
}

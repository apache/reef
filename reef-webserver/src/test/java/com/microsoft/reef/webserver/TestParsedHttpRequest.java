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
package com.microsoft.reef.webserver;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.io.bio.StringEndPoint;
import org.mortbay.jetty.*;

import javax.servlet.ServletException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public final class TestParsedHttpRequest {

  private Request request;
  private ParsedHttpRequest parsedRequest;

  @Before
  public void setUp() throws IOException, ServletException {

    this.request = new Request(
        new HttpConnection(new LocalConnector(), new StringEndPoint(), new Server()));

    this.request.setUri(new HttpURI("http://microsoft.com:8080/whatever/v1/id123#test?a=10&b=20&a=30"));
    this.request.setQueryString("a=10&b=20&a=30");
    this.request.setContentType("text/json");
    this.request.setPathInfo("/whatever/v1/id123#test");

    this.parsedRequest = new ParsedHttpRequest(this.request);
  }

  @Test
  public void testQueryMap() {
    Assert.assertEquals(
        new LinkedHashMap<String, List<String>>() {{
          put("a", Arrays.asList("10", "30"));
          put("b", Arrays.asList("20"));
        }},
        this.parsedRequest.getQueryMap());
  }

  @Test
  public void testHeaders() {
    Assert.assertEquals(
        new HashMap<String, String>() {{
          put("Content-Type", "text/json");
        }},
        this.parsedRequest.getHeaders());
  }

  @Test
  public void testTargetSpecification() {
    Assert.assertEquals("whatever", this.parsedRequest.getTargetSpecification());
  }

  @Test
  public void testVersion() {
    Assert.assertEquals("v1", this.parsedRequest.getVersion());
  }

  @Test
  public void testTargetEntity() {
    Assert.assertEquals("id123#test", this.parsedRequest.getTargetEntity());
  }

  @Test
  public void testPathInfo() {
    Assert.assertEquals("/whatever/v1/id123#test", this.parsedRequest.getPathInfo());
  }

  @Test
  public void testUrl() {
    Assert.assertEquals("http://microsoft.com:8080/whatever/v1/id123#test", this.parsedRequest.getRequestUrl());
  }

  @Test
  public void testUri() {
    Assert.assertEquals("/whatever/v1/id123#test", this.parsedRequest.getRequestUri());
  }
}

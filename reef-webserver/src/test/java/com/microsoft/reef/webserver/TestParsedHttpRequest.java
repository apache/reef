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
import org.junit.Test;

import org.mortbay.io.bio.StringEndPoint;
import org.mortbay.jetty.*;

import javax.servlet.ServletException;

import java.io.IOException;
import java.util.*;

public final class TestParsedHttpRequest {

  @Test
  public void testQueryMap() throws IOException, ServletException {

    final Request request = new Request(
        new HttpConnection(new LocalConnector(), new StringEndPoint(), new Server()));

    request.setUri(new HttpURI("http://localhost.com/get#test?a=10&b=20&a=30"));
    request.setQueryString("a=10&b=20&a=30");

    final ParsedHttpRequest parsedRequest = new ParsedHttpRequest(request);

    final Map<String, List<String>> queryMap = new LinkedHashMap<String, List<String>>(2) {{
      put("a", Arrays.asList("10", "30"));
      put("b", Arrays.asList("20"));
    }};

    Assert.assertEquals(queryMap, parsedRequest.getQueryMap());
  }
}

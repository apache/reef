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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * HttpHandler interface
 */
public interface HttpHandler {
  /**
   * return specification of the handler. e.g Reef
   *
   * @return
   */
  public String getUriSpecification();

  /**
   * return specification of the handler. e.g Reef
   *
   * @return
   */
  public void setUriSpecification(final String s);

  /**
   * Will be called when request comes
   *
   * @param parsedHttpRequest
   * @param response
   */
  void onHttpRequest(final ParsedHttpRequest parsedHttpRequest, final HttpServletResponse response) throws IOException, ServletException;
}

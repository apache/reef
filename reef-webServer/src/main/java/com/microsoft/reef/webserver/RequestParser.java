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
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * Parse HttpServletRequest
 */
public class RequestParser {
    private final HttpServletRequest request;
    private final String pathInfo;
    private final String method;
    private final String quesryString;
    private final String requestUri;
    private final String serveletPath;
    private final Map<String, String> headers = new HashMap();
    private final byte[] inputStream;
    private final String specification;
    private String targetEntity;

    /**
     * parse HttpServletRequest
     * @param request
     * @throws IOException
     * @throws ServletException
     */
    public  RequestParser(HttpServletRequest request) throws IOException, ServletException {
        this.request = request;

        pathInfo = request.getPathInfo();
        method = request.getMethod();
        quesryString = request.getQueryString();
        requestUri = request.getRequestURI();
        serveletPath = request.getServletPath();

        Enumeration hn = request.getHeaderNames();
        while (hn.hasMoreElements()){
            String s = (String)hn.nextElement();
            String header = request.getHeader(s);
            headers.put(s, header);
        }

        int len = request.getContentLength();
        if (len > 0)
        {
            inputStream = new byte[len];
            request.getInputStream().read(inputStream);
        } else {
            inputStream = null;
        }

        if (requestUri != null) {
            String[] parts = requestUri.split("/") ;

            if (parts != null && parts.length > 1) {
                specification = parts[1];
            } else {
                specification = null;
            }

            if (parts != null && parts.length > 2){
                targetEntity = parts[2];
            } else {
                targetEntity = null;
            }
        } else {
            specification = null;
            targetEntity = null;
        }
    }

    /**
     * get target to match specification like "Reef"
     * @return specification
     */
    public String getSpecification() {
        return specification;
    }

    /**
     * get query string like "id=12345"
     * @return
     */
    public String getQuesryString() {
        return quesryString;
    }

    /**
     * get target target entity like "Evaluators"
     * @return
     */
    public String getTargetEntity() {
        return targetEntity;
    }

    /**
     * get http request mathod like "Get"
     * @return
     */
    public String getMethod() {
        return method;
    }
}

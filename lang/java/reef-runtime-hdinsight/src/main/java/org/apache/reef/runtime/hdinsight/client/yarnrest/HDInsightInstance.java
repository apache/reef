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
package org.apache.reef.runtime.hdinsight.client.yarnrest;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.reef.runtime.hdinsight.parameters.HDInsightInstanceURL;
import org.apache.reef.runtime.hdinsight.parameters.HDInsightPassword;
import org.apache.reef.runtime.hdinsight.parameters.HDInsightUsername;
import org.apache.reef.tang.annotations.Parameter;
import org.codehaus.jackson.map.ObjectMapper;

import javax.inject.Inject;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents an HDInsight instance.
 */
public final class HDInsightInstance {

  private static final Logger LOG = Logger.getLogger(HDInsightInstance.class.getName());
  private static final String APPLICATION_KILL_MESSAGE = "{\"state\":\"KILLED\"}";

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Header[] headers;
  private final HttpClientContext httpClientContext;

  private final String instanceUrl;
  private final CloseableHttpClient httpClient;

  @Inject
  HDInsightInstance(@Parameter(HDInsightUsername.class) final String username,
                    @Parameter(HDInsightPassword.class) final String password,
                    @Parameter(HDInsightInstanceURL.class) final String instanceUrl,
                    final CloseableHttpClient client) throws URISyntaxException, IOException {
    this.httpClient = client;
    this.instanceUrl = instanceUrl.endsWith("/") ? instanceUrl : instanceUrl + "/";
    final String host = this.getHost();
    this.headers = new Header[]{
        new BasicHeader("Host", host)
    };
    this.httpClientContext = getClientContext(host, username, password);
  }

  /**
   * Request an ApplicationId from the cluster.
   *
   * @return
   * @throws IOException
   */
  public ApplicationID getApplicationID() throws IOException {
    final String url = "ws/v1/cluster/apps/new-application";
    final HttpPost post = preparePost(url);
    try (CloseableHttpResponse response = this.httpClient.execute(post, this.httpClientContext)) {
      final String message = IOUtils.toString(response.getEntity().getContent());
      final ApplicationID result = this.objectMapper.readValue(message, ApplicationID.class);
      return result;
    }
  }

  /**
   * Submits an application for execution.
   *
   * @param applicationSubmission
   * @throws IOException
   */
  public void submitApplication(final ApplicationSubmission applicationSubmission) throws IOException {
    final String url = "ws/v1/cluster/apps";
    final HttpPost post = preparePost(url);

    final StringWriter writer = new StringWriter();
    try {
      this.objectMapper.writeValue(writer, applicationSubmission);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    final String message = writer.toString();
    LOG.log(Level.FINE, "Sending:\n{0}", message.replace("\n", "\n\t"));
    post.setEntity(new StringEntity(message, ContentType.APPLICATION_JSON));

    try (CloseableHttpResponse response = this.httpClient.execute(post, this.httpClientContext)) {
      final String responseMessage = IOUtils.toString(response.getEntity().getContent());
      LOG.log(Level.FINE, "Response: {0}", responseMessage.replace("\n", "\n\t"));
    }
  }

  /**
   * Issues a YARN kill command to the application.
   *
   * @param applicationId
   */
  public void killApplication(final String applicationId) throws IOException {
    final String url = this.getApplicationURL(applicationId) + "/state";
    final HttpPut put = preparePut(url);
    put.setEntity(new StringEntity(APPLICATION_KILL_MESSAGE, ContentType.APPLICATION_JSON));
    this.httpClient.execute(put, this.httpClientContext);
  }

  /**
   * Gets the application state given a YARN application ID.
   * @param applicationId
   * @return Application state of the requested application.
   */
  public ApplicationState getApplication(final String applicationId) throws IOException {
    final String url = this.getApplicationURL(applicationId);
    final HttpGet get = prepareGet(url);
    try (CloseableHttpResponse response = this.httpClient.execute(get, this.httpClientContext)) {
      final String message = IOUtils.toString(response.getEntity().getContent());
      final ApplicationResponse result = this.objectMapper.readValue(message, ApplicationResponse.class);
      return result.getApplicationState();
    }
  }

  public List<ApplicationState> listApplications() throws IOException {
    final String url = "ws/v1/cluster/apps";
    final HttpGet get = prepareGet(url);
    try (CloseableHttpResponse response = this.httpClient.execute(get, this.httpClientContext)) {
      final String message = IOUtils.toString(response.getEntity().getContent());
      final ListApplicationResponse result = this.objectMapper.readValue(message, ListApplicationResponse.class);
      return result.getApplicationStates();
    }
  }

  /**
   * @param applicationId
   * @return the URL that can be used to issue application level messages.
   */
  public String getApplicationURL(final String applicationId) {
    return "ws/v1/cluster/apps/" + applicationId;
  }

  private String getHost() throws URISyntaxException {
    final URI uri = new URI(this.instanceUrl);
    return uri.getHost();
  }

  /**
   * Creates a HttpGet request with all the common headers.
   *
   * @param url
   * @return
   */
  private HttpGet prepareGet(final String url) {
    final HttpGet httpGet = new HttpGet(this.instanceUrl + url);
    for (final Header header : this.headers) {
      httpGet.addHeader(header);
    }
    return httpGet;
  }

  /**
   * Creates a HttpPost request with all the common headers.
   *
   * @param url
   * @return
   */
  private HttpPost preparePost(final String url) {
    final HttpPost httpPost = new HttpPost(this.instanceUrl + url);
    for (final Header header : this.headers) {
      httpPost.addHeader(header);
    }
    return httpPost;
  }

  /**
   * Creates a HttpPut request with all the common headers.
   * @param url
   * @return
   */
  private HttpPut preparePut(final String url) {
    final HttpPut httpPut = new HttpPut(this.instanceUrl + url);
    for (final Header header : this.headers) {
      httpPut.addHeader(header);
    }
    return httpPut;
  }

  private HttpClientContext getClientContext(final String hostname, final String username, final String password)
      throws IOException {
    final HttpHost targetHost = new HttpHost(hostname, 443, "https");
    final HttpClientContext result = HttpClientContext.create();

    // Setup credentials provider
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
    result.setCredentialsProvider(credentialsProvider);

    // Setup preemptive authentication
    final AuthCache authCache = new BasicAuthCache();
    final BasicScheme basicAuth = new BasicScheme();
    authCache.put(targetHost, basicAuth);
    result.setAuthCache(authCache);
    final HttpGet httpget = new HttpGet("/");

    // Prime the cache
    try (CloseableHttpResponse response = this.httpClient.execute(targetHost, httpget, result)) {
      // empty try block used to automatically close resources
    }
    return result;
  }
}

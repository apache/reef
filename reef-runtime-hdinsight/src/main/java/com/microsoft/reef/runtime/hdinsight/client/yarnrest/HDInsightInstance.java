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
package com.microsoft.reef.runtime.hdinsight.client.yarnrest;

import com.microsoft.reef.runtime.hdinsight.parameters.HDInsightInstanceURL;
import com.microsoft.reef.runtime.hdinsight.parameters.HDInsightPassword;
import com.microsoft.reef.runtime.hdinsight.parameters.HDInsightUsername;
import com.microsoft.tang.annotations.Parameter;
import org.apache.cxf.common.util.Base64Utility;
import org.codehaus.jackson.map.ObjectMapper;

import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents an HDInsight instance.
 */
public final class HDInsightInstance {

  private static final Logger LOG = Logger.getLogger(HDInsightInstance.class.getName());
  private static final String APPLICATION_KILL_MESSAGE = "{\"app:{\"state\":\"KILLED\"}}";

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();


  /**
   * e.g. https://reefhdi.cloudapp.net/
   */
  private final String instanceUrl;
  private final Client client;
  private final String username;

  @Inject
  HDInsightInstance(final @Parameter(HDInsightUsername.class) String username,
                    final @Parameter(HDInsightPassword.class) String password,
                    final @Parameter(HDInsightInstanceURL.class) String instanceUrl,
                    final Client client) {
    this.instanceUrl = instanceUrl.endsWith("/") ? instanceUrl : instanceUrl + "/";
    this.client = client;
    this.username = username;
    this.headers.add("Authorization",
        "Basic " + Base64Utility.encode((username + ":" + password).getBytes()));
  }


  public ApplicationID getApplicationID() throws IOException {
    final Invocation.Builder b = getInvocationBuilder("ws/v1/cluster/appids?user.name=" + this.username);
    final Response response = b.post(null);
    return this.objectMapper.readValue((InputStream) response.getEntity(), ApplicationID.class);
  }

  /**
   * Submits an application for execution.
   *
   * @param applicationSubmission
   * @throws IOException
   */
  public void submitApplication(final ApplicationSubmission applicationSubmission) throws IOException {

    final String applicationId = applicationSubmission.getApplicationId();
    final String url = "ws/v1/cluster/apps/" + applicationId + "?user.name=" + this.username;
    final Invocation.Builder b = getInvocationBuilder(url);

    final StringWriter writer = new StringWriter();

    try {
      this.objectMapper.writeValue(writer, applicationSubmission);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final String message = writer.toString();
    LOG.log(Level.FINE, "Sending:\n{0}", message.replace("\n", "\n\t"));

    final Response response = b.post(Entity.entity(message, MediaType.APPLICATION_JSON_TYPE));
    LOG.log(Level.FINEST, "Response: {0}", response);
  }

  /**
   * Issues a YARN kill command to the application.
   *
   * @param applicationId
   */
  public void killApplication(final String applicationId) {
    LOG.log(Level.INFO, "Killing application [{0}]", applicationId);
    getInvocationBuilder(getApplicationURL(applicationId)).post(Entity.entity(APPLICATION_KILL_MESSAGE, MediaType.APPLICATION_JSON_TYPE));
  }

  /**
   * @param applicationId
   * @return the URL that can be used to issue application level messages.
   */
  public String getApplicationURL(final String applicationId) {
    return "ws/v1/cluster/apps/" + applicationId;
  }

  private Invocation.Builder getInvocationBuilder(final String path) {
    final WebTarget target = this.client.target(this.instanceUrl + path);
    final Invocation.Builder b = target.request();
    b.headers(this.headers);
    return b;
  }
}

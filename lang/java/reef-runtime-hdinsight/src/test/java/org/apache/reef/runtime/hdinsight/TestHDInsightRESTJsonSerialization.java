/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runtime.hdinsight;

import org.apache.reef.runtime.hdinsight.client.yarnrest.*;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * These tests apply to REST calls to the YARN Resource Manager
 * for HDInsight 3.2, which corresponds to Hadoop version 2.6.0.
 * Details are documented at:
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
public final class TestHDInsightRESTJsonSerialization {

  @Test
  public void testSerializeApplicationSubmission() throws IOException {
    // Define expected values
    final Map<String, LocalResource> localResourceMap = new HashMap<>();
    final String archiveKey = "archive";
    final String fileKey = "file";

    // Create submission object
    final LocalResource archiveResource = new LocalResource()
        .setResource("archiveResourceLocation").setSize(100).setTimestamp(200)
        .setType(LocalResource.TYPE_ARCHIVE).setVisibility(LocalResource.VISIBILITY_PRIVATE);
    localResourceMap.put(archiveKey, archiveResource);

    final LocalResource fileResource = new LocalResource()
        .setResource("fileResourceLocation").setSize(300).setTimestamp(400)
        .setType(LocalResource.TYPE_FILE).setVisibility(LocalResource.VISIBILITY_APPLICATION);
    localResourceMap.put(fileKey, fileResource);

    final Credentials creds = new Credentials().addSecret("secretKey", "secretVal").addToken("tokKey", "tokVal");
    final AmContainerSpec containerSpec = new AmContainerSpec()
        .setCommand("submission command")
        .addLocalResource(archiveKey, archiveResource)
        .addLocalResource(fileKey, fileResource)
        .addEnvironment("envKey", "envVal")
        .addApplicationAcl("aclKey", "aclVal")
        .addServiceData("sdKey", "sdVal")
        .setCredentials(creds);
    final Resource resource = new Resource().setMemory(500).setvCores(600);
    final ApplicationSubmission submission = new ApplicationSubmission()
        .setApplicationType(ApplicationSubmission.DEFAULT_APPLICATION_TYPE)
        .setMaxAppAttempts(ApplicationSubmission.DEFAULT_MAX_APP_ATTEMPTS)
        .setKeepContainers(ApplicationSubmission.DEFAULT_KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS)
        .setQueue(ApplicationSubmission.DEFAULT_QUEUE)
        .setPriority(ApplicationSubmission.DEFAULT_PRIORITY)
        .setUnmanagedAM(ApplicationSubmission.DEFAULT_UNMANAGED_AM)
        .setAmContainerSpec(containerSpec).setApplicationId("appId").setApplicationName("name")
        .setResource(resource);

    // Json validation
    final ObjectMapper mapper = new ObjectMapper();
    final StringWriter writer = new StringWriter();
    mapper.writeValue(writer, submission);
    final String jsonStr = writer.toString();
    final JsonNode rootJsonNode = mapper.readTree(jsonStr);
    Assert.assertEquals(rootJsonNode.get(Constants.APPLICATION_ID).asText(), submission.getApplicationId());
    Assert.assertEquals(rootJsonNode.get(Constants.APPLICATION_NAME).asText(), submission.getApplicationName());
    Assert.assertEquals(rootJsonNode.get(Constants.MAX_APP_ATTEMPTS).asInt(), submission.getMaxAppAttempts());
    Assert.assertEquals(rootJsonNode.get(Constants.KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS).asBoolean(),
        submission.isKeepContainers());
    Assert.assertEquals(rootJsonNode.get(Constants.QUEUE).asText(), submission.getQueue());
    Assert.assertEquals(rootJsonNode.get(Constants.PRIORITY).asInt(), submission.getPriority());
    Assert.assertEquals(rootJsonNode.get(Constants.UNMANAGED_AM).asBoolean(), submission.isUnmanagedAM());

    final JsonNode resourceNode = rootJsonNode.get(Constants.RESOURCE);
    Assert.assertNotNull(resourceNode);
    Assert.assertEquals(resourceNode.get(Constants.MEMORY).asInt(), resource.getMemory());
    Assert.assertEquals(resourceNode.get(Constants.VCORES).asInt(), resource.getvCores());

    final JsonNode amSpecNode = rootJsonNode.get(Constants.AM_CONTAINER_SPEC);
    Assert.assertNotNull(amSpecNode);
    Assert.assertEquals(amSpecNode.get(Constants.COMMANDS).get(Constants.COMMAND).asText(),
        containerSpec.getCommands().getCommand());
    final JsonNode locResourcesNode = amSpecNode.get(Constants.LOCAL_RESOURCES).get(Constants.ENTRY);
    Assert.assertTrue(locResourcesNode.isArray());
    for (final JsonNode localResourceKVNode : locResourcesNode) {
      final String localResourceKey = localResourceKVNode.get(Constants.KEY).asText();
      Assert.assertTrue(localResourceMap.containsKey(localResourceKey));
      final LocalResource localResourceFromMap = localResourceMap.get(localResourceKey);
      final JsonNode localResourceNode = localResourceKVNode.get(Constants.VALUE);
      Assert.assertEquals(localResourceNode.get(Constants.RESOURCE).asText(), localResourceFromMap.getResource());
      Assert.assertEquals(localResourceNode.get(Constants.SIZE).asLong(), localResourceFromMap.getSize());
      Assert.assertEquals(localResourceNode.get(Constants.TIMESTAMP).asLong(), localResourceFromMap.getTimestamp());
      Assert.assertEquals(localResourceNode.get(Constants.TYPE).asText(), localResourceFromMap.getType());
      Assert.assertEquals(localResourceNode.get(Constants.VISIBILITY).asText(), localResourceFromMap.getVisibility());
      localResourceMap.remove(localResourceKey);
    }

    Assert.assertTrue(localResourceMap.isEmpty());

    final JsonNode credsNode = amSpecNode.get(Constants.CREDENTIALS);
    Assert.assertNotNull(credsNode);
    final JsonNode toksNode = credsNode.get(Constants.TOKENS).get(Constants.ENTRY);
    Assert.assertNotNull(toksNode);
    Assert.assertTrue(toksNode.isArray());
    for (final JsonNode tokKVNode : toksNode) {
      final StringEntry tokenEntry = containerSpec.getCredentials().getTokens().get(Constants.ENTRY).get(0);
      Assert.assertEquals(tokKVNode.get(Constants.KEY).asText(), tokenEntry.getKey());
      Assert.assertEquals(tokKVNode.get(Constants.VALUE).asText(), tokenEntry.getValue());
    }
    final JsonNode secretsNode = credsNode.get(Constants.SECRETS).get(Constants.ENTRY);
    Assert.assertNotNull(secretsNode);
    Assert.assertTrue(secretsNode.isArray());
    for (final JsonNode secretsKVNode : secretsNode) {
      final StringEntry secretsEntry = containerSpec.getCredentials().getSecrets().get(Constants.ENTRY).get(0);
      Assert.assertEquals(secretsKVNode.get(Constants.KEY).asText(), secretsEntry.getKey());
      Assert.assertEquals(secretsKVNode.get(Constants.VALUE).asText(), secretsEntry.getValue());
    }

    final JsonNode envsNode = amSpecNode.get(Constants.ENVIRONMENT).get(Constants.ENTRY);
    Assert.assertNotNull(envsNode);
    Assert.assertTrue(envsNode.isArray());
    for (final JsonNode envsKVNode : envsNode) {
      final StringEntry envEntry = containerSpec.getEnvironment().get(Constants.ENTRY).get(0);
      Assert.assertEquals(envsKVNode.get(Constants.KEY).asText(), envEntry.getKey());
      Assert.assertEquals(envsKVNode.get(Constants.VALUE).asText(), envEntry.getValue());
    }

    final JsonNode aclsNode = amSpecNode.get(Constants.APPLICATION_ACLS).get(Constants.ENTRY);
    Assert.assertNotNull(aclsNode);
    Assert.assertTrue(aclsNode.isArray());
    for (final JsonNode aclsKVNode : aclsNode) {
      final StringEntry aclsEntry = containerSpec.getApplicationAcls().get(Constants.ENTRY).get(0);
      Assert.assertEquals(aclsKVNode.get(Constants.KEY).asText(), aclsEntry.getKey());
      Assert.assertEquals(aclsKVNode.get(Constants.VALUE).asText(), aclsEntry.getValue());
    }

    final JsonNode sdatasNode = amSpecNode.get(Constants.SERVICE_DATA).get(Constants.ENTRY);
    Assert.assertNotNull(sdatasNode);
    Assert.assertTrue(sdatasNode.isArray());
    for (final JsonNode sdataKVNode : sdatasNode) {
      final StringEntry sdataEntry = containerSpec.getServiceData().get(Constants.ENTRY).get(0);
      Assert.assertEquals(sdataKVNode.get(Constants.KEY).asText(), sdataEntry.getKey());
      Assert.assertEquals(sdataKVNode.get(Constants.VALUE).asText(), sdataEntry.getValue());
    }
  }

  @Test
  public void testDeserializeGetApplicationId() throws IOException {
    final String appIdStr = "application_1404198295326_0003";
    final int memory = 8192;
    final int vCores = 32;

    final String getAppIdBody = "{\n" +
        "  \"application-id\":\"" + appIdStr + "\",\n" +
        "  \"maximum-resource-capability\":\n" +
        "    {\n" +
        "      \"memory\":" + memory + ",\n" +
        "      \"vCores\":" + vCores + "\n" +
        "    }\n" +
        "}";

    final ApplicationID appId = new ObjectMapper().readValue(getAppIdBody, ApplicationID.class);
    Assert.assertEquals(appId.getApplicationId(), appIdStr);
    Assert.assertEquals(appId.getResource().getMemory(), memory);
    Assert.assertEquals(appId.getResource().getvCores(), vCores);
  }

  @Test
  public void testDeserializeGetApplication() throws IOException {
    final long finishedTime = 1326824991300L;
    final String amContainerLogs =
        "http://host.domain.com:8042/node/containerlogs/container_1326821518301_0005_01_000001";
    final String trackingUI = "History";
    final String state = "FINISHED";
    final String user = "user1";
    final String appId = "application_1326821518301_0005";
    final String clusterId = "1326821518301";
    final String finalStatus = "SUCCEEDED";
    final String amHostHttpAddress = "host.domain.com:8042";
    final String progress = "100";
    final String name = "Sleep job";
    final String applicationType = "Yarn";
    final long startedTime = 1326824544552L;
    final long elapsedTime = 446748L;
    final String diagnostics = "";
    final String trackingUrl =
        "http://host.domain.com:8088/proxy/application_1326821518301_0005/jobhistory/job/job_1326821518301_5_5";
    final String queue = "a1";
    final long memorySeconds = 151730L;
    final long vcoreSeconds = 103L;

    final String getAppBody = "{\n" +
        "   \"app\" : {\n" +
        "      \"finishedTime\" : " + finishedTime + ",\n" +
        "      \"amContainerLogs\" : \"" + amContainerLogs + "\",\n" +
        "      \"trackingUI\" : \"" + trackingUI + "\",\n" +
        "      \"state\" : \"" + state + "\",\n" +
        "      \"user\" : \"" + user + "\",\n" +
        "      \"id\" : \"" + appId + "\",\n" +
        "      \"clusterId\" : " + clusterId + ",\n" +
        "      \"finalStatus\" : \"" + finalStatus + "\",\n" +
        "      \"amHostHttpAddress\" : \"" + amHostHttpAddress + "\",\n" +
        "      \"progress\" : " + progress + ",\n" +
        "      \"name\" : \"" + name + "\",\n" +
        "      \"applicationType\" : \"" + applicationType + "\",\n" +
        "      \"startedTime\" : " + startedTime + ",\n" +
        "      \"elapsedTime\" : " + elapsedTime + ",\n" +
        "      \"diagnostics\" : \"" + diagnostics + "\",\n" +
        "      \"trackingUrl\" : \"" + trackingUrl + "\",\n" +
        "      \"queue\" : \"" + queue + "\",\n" +
        "      \"memorySeconds\" : " + memorySeconds + ",\n" +
        "      \"vcoreSeconds\" : " + vcoreSeconds + "\n" +
        "   }" +
        "}";

    final ApplicationResponse appResponse = new ObjectMapper().readValue(getAppBody, ApplicationResponse.class);
    final ApplicationState appState = appResponse.getApplicationState();
    Assert.assertEquals(appState.getFinishedTime(), finishedTime);
    Assert.assertEquals(appState.getAmContainerLogs(), amContainerLogs);
    Assert.assertEquals(appState.getTrackingUI(), trackingUI);
    Assert.assertEquals(appState.getState(), state);
    Assert.assertEquals(appState.getUser(), user);
    Assert.assertEquals(appState.getId(), appId);
    Assert.assertEquals(appState.getClusterId(), clusterId);
    Assert.assertEquals(appState.getFinalStatus(), finalStatus);
    Assert.assertEquals(appState.getAmHostHttpAddress(), amHostHttpAddress);
    Assert.assertEquals(appState.getProgress(), progress);
    Assert.assertEquals(appState.getName(), name);
    Assert.assertEquals(appState.getApplicationType(), applicationType);
    Assert.assertEquals(appState.getStartedTime(), startedTime);
    Assert.assertEquals(appState.getElapsedTime(), elapsedTime);
    Assert.assertEquals(appState.getDiagnostics(), diagnostics);
    Assert.assertEquals(appState.getTrackingUrl(), trackingUrl);
    Assert.assertEquals(appState.getQueue(), queue);
    Assert.assertEquals(appState.getMemorySeconds(), memorySeconds);
    Assert.assertEquals(appState.getVCoreSeconds(), vcoreSeconds);
  }

  @Test
  public void testDeserializeListApplications() throws IOException {
    final String listAppsBody = "{\n" +
        "  \"apps\":\n" +
        "  {\n" +
        "    \"app\":\n" +
        "    [\n" +
        "       {\n" +
        "          \"finishedTime\" : 1326815598530,\n" +
        "          \"amContainerLogs\" : " +
        "\"http://host.domain.com:8042/node/containerlogs/container_1326815542473_0001_01_000001\",\n" +
        "          \"trackingUI\" : \"History\",\n" +
        "          \"state\" : \"FINISHED\",\n" +
        "          \"user\" : \"user1\",\n" +
        "          \"id\" : \"application_1326815542473_0001\",\n" +
        "          \"clusterId\" : 1326815542473,\n" +
        "          \"finalStatus\" : \"SUCCEEDED\",\n" +
        "          \"amHostHttpAddress\" : \"host.domain.com:8042\",\n" +
        "          \"progress\" : 100,\n" +
        "          \"name\" : \"word count\",\n" +
        "          \"startedTime\" : 1326815573334,\n" +
        "          \"elapsedTime\" : 25196,\n" +
        "          \"diagnostics\" : \"\",\n" +
        "          \"trackingUrl\" : " +
        "\"http://host.domain.com:8088/proxy/application_1326815542473_0001/jobhistory/job/job_1326815542473_1_1\",\n" +
        "          \"queue\" : \"default\",\n" +
        "          \"allocatedMB\" : 0,\n" +
        "          \"allocatedVCores\" : 0,\n" +
        "          \"runningContainers\" : 0,\n" +
        "          \"memorySeconds\" : 151730,\n" +
        "          \"vcoreSeconds\" : 103\n" +
        "       },\n" +
        "       {\n" +
        "          \"finishedTime\" : 1326815789546,\n" +
        "          \"amContainerLogs\" : " +
        "\"http://host.domain.com:8042/node/containerlogs/container_1326815542473_0002_01_000001\",\n" +
        "          \"trackingUI\" : \"History\",\n" +
        "          \"state\" : \"FINISHED\",\n" +
        "          \"user\" : \"user1\",\n" +
        "          \"id\" : \"application_1326815542473_0002\",\n" +
        "          \"clusterId\" : 1326815542473,\n" +
        "          \"finalStatus\" : \"SUCCEEDED\",\n" +
        "          \"amHostHttpAddress\" : \"host.domain.com:8042\",\n" +
        "          \"progress\" : 100,\n" +
        "          \"name\" : \"Sleep job\",\n" +
        "          \"startedTime\" : 1326815641380,\n" +
        "          \"elapsedTime\" : 148166,\n" +
        "          \"diagnostics\" : \"\",\n" +
        "          \"trackingUrl\" : " +
        "\"http://host.domain.com:8088/proxy/application_1326815542473_0002/jobhistory/job/job_1326815542473_2_2\",\n" +
        "          \"queue\" : \"default\",\n" +
        "          \"allocatedMB\" : 0,\n" +
        "          \"allocatedVCores\" : 0,\n" +
        "          \"runningContainers\" : 1,\n" +
        "          \"memorySeconds\" : 640064,\n" +
        "          \"vcoreSeconds\" : 442\n" +
        "       } \n" +
        "    ]\n" +
        "  }\n" +
        "}";

    final ListApplicationResponse listAppsResponse =
        new ObjectMapper().readValue(listAppsBody, ListApplicationResponse.class);
    Assert.assertTrue(listAppsResponse.getApps().containsKey(Constants.APP));
    Assert.assertEquals(listAppsResponse.getApplicationStates().size(), 2);
    for (final ApplicationState state : listAppsResponse.getApplicationStates()) {
      Assert.assertNotNull(state);
    }
  }
}

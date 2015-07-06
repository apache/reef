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

/**
 * Constants used in serializing/deserializing REST calls to HDInsight's
 * Resource Manager.
 * For detailed information, please refer to
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
public final class Constants {
  public static final String ID = "id";
  public static final String MAXIMUM_RESOURCE_CAPABILITY = "maximum-resource-capability";
  public static final String APPLICATION_ID = "application-id";
  public static final String APPLICATION_NAME = "application-name";
  public static final String UNMANAGED_AM = "unmanaged-AM";
  public static final String MAX_APP_ATTEMPTS = "max-app-attempts";
  public static final String APPLICATION_TYPE = "application-type";
  public static final String AM_CONTAINER_SPEC = "am-container-spec";
  public static final String KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS = "keep-containers-across-application-attempts";
  public static final String APPLICATION_TAGS = "application-tags";
  public static final String QUEUE = "queue";
  public static final String RESOURCE = "resource";
  public static final String PRIORITY = "priority";
  public static final String LOCAL_RESOURCES = "local-resources";
  public static final String ENVIRONMENT = "environment";
  public static final String COMMANDS = "commands";
  public static final String COMMAND = "command";
  public static final String ENTRY = "entry";
  public static final String KEY = "key";
  public static final String VALUE = "value";
  public static final String APPLICATION_ACLS = "application-acls";
  public static final String SERVICE_DATA = "service-data";
  public static final String CREDENTIALS = "credentials";
  public static final String SECRETS = "secrets";
  public static final String TOKENS = "tokens";
  public static final String TYPE = "type";
  public static final String VISIBILITY = "visibility";
  public static final String SIZE = "size";
  public static final String TIMESTAMP = "timestamp";
  public static final String MEMORY = "memory";
  public static final String VCORES = "vCores";
  public static final String APPS = "apps";
  public static final String APP = "app";
  public static final String FINISHED_TIME = "finishedTime";
  public static final String AM_CONTAINER_LOGS = "amContainerLogs";
  public static final String TRACKING_UI = "trackingUI";
  public static final String RESPONSE_APPLICATION_TYPE = "applicationType";
  public static final String STATE = "state";
  public static final String USER = "user";
  public static final String CLUSTER_ID = "clusterId";
  public static final String FINAL_STATUS = "finalStatus";
  public static final String AM_HOST_HTTP_ADDRESS = "amHostHttpAddress";
  public static final String PROGRESS = "progress";
  public static final String NAME = "name";
  public static final String STARTED_TIME = "startedTime";
  public static final String ELAPSED_TIME = "elapsedTime";
  public static final String DIAGNOSTICS = "diagnostics";
  public static final String TRACKING_URL = "trackingUrl";
  public static final String ALLOCATED_MB = "allocatedMB";
  public static final String ALLOCATED_VCORES = "allocatedVCores";
  public static final String RUNNING_CONTAINERS = "runningContainers";
  public static final String MEMORY_SECONDS = "memorySeconds";
  public static final String VCORE_SECONDS = "vcoreSeconds";

  private Constants() {
  }
}

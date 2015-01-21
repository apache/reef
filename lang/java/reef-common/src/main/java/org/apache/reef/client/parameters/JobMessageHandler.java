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
package org.apache.reef.client.parameters;

import org.apache.reef.client.JobMessage;
import org.apache.reef.runtime.common.client.defaults.DefaultJobMessageHandler;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.EventHandler;

/**
 * Client EventHandler that gets messages from the Driver.
 */
@NamedParameter(doc = "Client EventHandler that gets messages from the Driver.",
    default_classes = DefaultJobMessageHandler.class)
public final class JobMessageHandler implements Name<EventHandler<JobMessage>> {
  private JobMessageHandler() {
  }
}

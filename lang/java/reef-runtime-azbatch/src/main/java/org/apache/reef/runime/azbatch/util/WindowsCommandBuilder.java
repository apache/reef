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
package org.apache.reef.runime.azbatch.util;

import org.apache.commons.lang.NotImplementedException;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;

import java.util.Collections;
import java.util.List;

/**
 * Build the launch command for Java REEF processes for Azure Batch Windows pools.
 */
public class WindowsCommandBuilder implements CommandBuilder {

  private static final char CLASSPATH_SEPARATOR_CHAR = ';';

  private static final List<String> COMMAND_LIST_PREFIX = Collections.emptyList();

  private static final String BATCH_COMMAND_LINE_FORMAT = "cmd /c \"%s\"";

  @Override
  public String build(final JobSubmissionEvent jobSubmissionEvent) {
    throw new NotImplementedException();
  }
}

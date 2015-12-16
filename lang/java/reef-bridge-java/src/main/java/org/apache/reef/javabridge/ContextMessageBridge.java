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
package org.apache.reef.javabridge;

import org.apache.commons.lang.NotImplementedException;
import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ContextMessage;

/**
 * The Java-CLR bridge object for {@link org.apache.reef.driver.context.ContextMessage}.
 */
@Private
@Interop(
    CppFiles = { "Clr2JavaImpl.h", "ContextMessageClr2Java.cpp" },
    CsFiles = { "IContextMessageClr2Java.cs", "ContextMessage.cs" })
public final class ContextMessageBridge extends NativeBridge implements ContextMessage {

  private ContextMessage jcontextMessage;
  private String contextMessageId;
  private String messageSourceId;
  private byte[] message;

  public ContextMessageBridge(final ContextMessage contextMessage) {
    jcontextMessage = contextMessage;
    contextMessageId = contextMessage.getId();
    messageSourceId = contextMessage.getMessageSourceID();
    message = contextMessage.get();
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public byte[] get() {
    return message;
  }

  @Override
  public String getId() {
    return contextMessageId;
  }

  @Override
  public String getMessageSourceID() {
    return messageSourceId;
  }

  @Override
  public long getSequenceNumber(){
    // TODO[REEF-1085] once REEF.NET supports sequence numbers, ensure the numbers
    // can propagate between C# and Java implementations
    throw new NotImplementedException("A Java-CLR bridge lacks support of sequence numbers on the REEF.NET side.");
  }
}

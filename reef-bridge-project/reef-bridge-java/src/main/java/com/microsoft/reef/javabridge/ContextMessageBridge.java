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

package com.microsoft.reef.javabridge;

import com.microsoft.reef.driver.context.ContextMessage;

public class ContextMessageBridge extends NativeBridge implements ContextMessage {

  private ContextMessage jcontextMessage;
  private String contextMessageId;
  private String messageSourceId;
  private byte[] message;

  public ContextMessageBridge(ContextMessage contextMessage)
  {
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
}

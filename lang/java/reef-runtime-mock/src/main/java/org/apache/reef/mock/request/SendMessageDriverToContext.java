/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.reef.mock.request;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.mock.ProcessRequest;

/**
 * send message from driver to context process request.
 */
public class SendMessageDriverToContext implements
    ProcessRequestInternal<Object, Object> {

  private final ActiveContext context;

  private final byte[] message;

  public SendMessageDriverToContext(final ActiveContext context, final byte[] message) {
    this.context = context;
    this.message = message;
  }

  @Override
  public Type getType() {
    return Type.SEND_MESSAGE_DRIVER_TO_CONTEXT;
  }

  public ActiveContext getContext() {
    return this.context;
  }

  public byte[] getMessage() {
    return this.message;
  }

  @Override
  public Object getSuccessEvent() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getFailureEvent() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean doAutoComplete() {
    return false;
  }

  @Override
  public void setAutoComplete(final boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ProcessRequest getCompletionProcessRequest() {
    throw new UnsupportedOperationException();
  }
}

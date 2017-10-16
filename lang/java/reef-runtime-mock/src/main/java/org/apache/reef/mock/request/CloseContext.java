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

import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.mock.AutoCompletable;
import org.apache.reef.mock.ProcessRequest;
import org.apache.reef.mock.runtime.MockActiveContext;
import org.apache.reef.mock.runtime.MockClosedContext;
import org.apache.reef.mock.runtime.MockFailedContext;

/**
 * close context process request.
 */
public class CloseContext implements
    ProcessRequestInternal<ClosedContext, FailedContext>,
    AutoCompletable {

  private final MockActiveContext context;

  public CloseContext(final MockActiveContext context) {
    this.context = context;
  }

  @Override
  public Type getType() {
    return Type.CLOSE_CONTEXT;
  }

  @Override
  public MockClosedContext getSuccessEvent() {
    return new MockClosedContext(this.context);
  }

  @Override
  public FailedContext getFailureEvent() {
    return new MockFailedContext(this.context);
  }

  @Override
  public boolean doAutoComplete() {
    return !this.context.getParentContext().isPresent();
  }

  @Override
  public void setAutoComplete(final boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ProcessRequest getCompletionProcessRequest() {
    return new CloseEvaluator(this.context.getEvaluator());
  }
}

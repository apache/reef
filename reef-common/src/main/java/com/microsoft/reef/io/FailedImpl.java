/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.io;

import com.microsoft.reef.util.Optional;

import java.io.PrintWriter;
import java.io.StringWriter;

public abstract class FailedImpl implements Failed {

  protected final String id;

  protected final String message;

  protected final Optional<String> description;

  protected final Optional<Throwable> cause;

  /**
   * Serialized error message.
   * TODO: populate that field later.
   */
  protected final Optional<byte[]> data = Optional.empty();

  public FailedImpl(final String id, final String message) {
    this(id, message, null, null);
  }

  public FailedImpl(final String id, final String message, final String description) {
    this(id, message, description, null);
  }

  public FailedImpl(final String id, final String message,
                    final String description, final Throwable cause) {
    assert (id != null);
    assert (message != null);
    this.id = id;
    this.message = message;
    this.description = Optional.ofNullable(description != null ? description : getStackTrace(cause));
    this.cause = Optional.ofNullable(cause);
  }

  public FailedImpl(final String id, final Throwable cause) {
    assert (id != null);
    this.id = id;
    this.cause = Optional.of(cause);
    this.message = cause.getMessage();
    this.description = Optional.of(getStackTrace(cause));
  }

  private static String getStackTrace(final Throwable cause) {
    if (cause == null) {
      return null;
    } else {
      final StringWriter writer = new StringWriter();
      cause.printStackTrace(new PrintWriter(writer));
      return writer.toString();
    }
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public String getMessage() {
    return this.message;
  }

  @Override
  public Optional<String> getDescription() {
    return this.description;
  }

  @Override
  public Throwable getCause() {
    return this.cause.orElse(null);
  }

  @Override
  public Optional<byte[]> getData() {
    return this.data;
  }

  /**
   * FIXME: replace RuntimeException with a better class.
   * @return ALWAYS returns an exception.
   */
  @Override
  public Throwable asError() {
    return this.cause.isPresent() ? this.cause.get() : new RuntimeException(this.message);
  }

  @Override
  public String toString() {
    return this.getClass().getName() + " id=" + this.getId() + " :: " + this.getMessage();
  }
}

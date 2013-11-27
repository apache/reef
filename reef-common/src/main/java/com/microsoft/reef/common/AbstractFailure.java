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
package com.microsoft.reef.common;

import com.microsoft.reef.util.Optional;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Common data and functionality for all error messages in REEF.
 */
public abstract class AbstractFailure implements Failure {

  /**
   * Identifier of the entity that produced the error. Cannot be null.
   */
  protected final String id;

  /**
   * One-line error message. For wrapped exceptions, this equals
   * to the Exception.getMessage() result. Cannot be null.
   */
  protected final String message;

  /**
   * Optional error description (long).
   * For exceptions it is by default populates with the stack trace.
   */
  protected final Optional<String> description;

  /**
   * Optional Java exception that caused the error.
   */
  protected final Optional<Throwable> cause;

  /**
   * Optional byte array that contains serialized version of the exception.
   */
  protected final Optional<byte[]> data;

  /**
   * Minimal constructor: Build error message given the entity ID and the short error message.
   *
   * @param id Identifier of the entity that produced the error. Cannot be null.
   * @param message One-line error message. Cannot be null.
   */
  public AbstractFailure(final String id, final String message) {
    this(id, message, null, null, null);
  }

  /**
   * Build error message given the entity ID plus short and long error message.
   *
   * @param id Identifier of the entity that produced the error. Cannot be null.
   * @param message One-line error message. Cannot be null.
   * @param description Long error description. Can be null.
   */
  public AbstractFailure(final String id, final String message, final String description) {
    this(id, message, description, null, null);
  }

  /**
   * Most detailed error message constructor that takes all parameters possible.
   *
   * @param id Identifier of the entity that produced the error. Cannot be null.
   * @param message One-line error message. Cannot be null.
   * @param description Long error description. Can be null.
   * @param cause Java Exception that caused the error. Can be null.
   * @param data byte array that contains serialized version of the error. Can be null.
   */
  public AbstractFailure(final String id, final String message, final String description,
                         final Throwable cause, final byte[] data) {
    assert (id != null);
    assert (message != null);
    this.id = id;
    this.message = message;
    this.description = Optional.ofNullable(description != null ? description : getStackTrace(cause));
    this.cause = Optional.ofNullable(cause);
    this.data = Optional.ofNullable(data);
  }

  /**
   * Build error message given the failed entity ID and Java Exception.
   * Populates the message with the Exception.getMessage() result, and stores
   * the exception stack trace in the description.
   * @param id Identifier of the entity that produced the error. Cannot be null.
   * @param cause Java Exception that caused the error. Cannot be null.
   */
  public AbstractFailure(final String id, final Throwable cause) {
    assert (id != null);
    this.id = id;
    this.cause = Optional.of(cause);
    this.message = cause.getMessage();
    this.description = Optional.of(getStackTrace(cause));
    this.data = Optional.empty();
  }

  /**
   * Helper function: produce the string that contains the given exception's stack trace.
   * Returns null if the argument is null.
   * @param cause Java Exception or null.
   * @return A string that contains the exception stack trace, or null.
   */
  protected static String getStackTrace(final Throwable cause) {
    if (cause == null) {
      return null;
    } else {
      final StringWriter writer = new StringWriter();
      cause.printStackTrace(new PrintWriter(writer));
      return writer.toString();
    }
  }

  /**
   * @return Identifier of the entity that produced the error. Never null.
   */
  @Override
  public String getId() {
    return this.id;
  }

  /**
   * @return One-line error message. Never null.
   */
  @Override
  public String getMessage() {
    return this.message;
  }

  /**
   * @return Optional long error description. For Java Exceptions, can contain stack trace.
   */
  @Override
  public Optional<String> getDescription() {
    return this.description;
  }

  /**
   * @return Java Exception that caused the error, or null.
   */
  @Override
  public Throwable getCause() {
    return this.cause.orElse(null);
  }

  /**
   * @return Optional serialized version of the error message.
   */
  @Override
  public Optional<byte[]> getData() {
    return this.data;
  }

  /**
   * Return the original Java Exception, or generate a new one if it does not exists.
   * ALWAYS returns an exception.
   * FIXME: Replace RuntimeException with a better class.
   * @return A java exception. Never null.
   */
  @Override
  public Throwable asError() {
    return this.cause.isPresent() ? this.cause.get() : new RuntimeException(this.toString());
  }

  /**
   * @return Human-readable string representation of an error message.
   */
  @Override
  public String toString() {
    return this.getClass().getName() + " id=" + this.getId() + " failed: " + this.getMessage();
  }
}

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
package com.microsoft.reef.runtime.common.evaluator.context;

import com.microsoft.reef.evaluator.context.parameters.ContextIdentifier;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;

/**
 * Thrown when we encounter a problem with client code in a context.
 */
public final class ContextClientCodeException extends Exception {
  private final String contextID;
  private final Optional<String> parentID;

  /**
   * @param contextID the ID of the failed context.
   * @param parentID  the ID of the failed context's parent, if any.
   * @param message   the error message.
   * @param cause     the exception that caused the error.
   */
  public ContextClientCodeException(final String contextID,
                                    final Optional<String> parentID,
                                    final String message,
                                    final Throwable cause) {
    super("Failure in context '" + contextID + "': " + message, cause);
    this.contextID = contextID;
    this.parentID = parentID;
  }

  /**
   * @return the ID of the failed context
   */
  public String getContextID() {
    return this.contextID;
  }

  /**
   * @return the ID of the failed context's parent, if any
   */
  public Optional<String> getParentID() {
    return this.parentID;
  }

  /**
   * Extracts a context id from the given configuration.
   *
   * @param c
   * @return the context id in the given configuration.
   * @throws RuntimeException if the configuration can't be parsed.
   */
  public static String getIdentifier(final Configuration c) {
    try {
      return Tang.Factory.getTang().newInjector(c).getNamedInstance(
          ContextIdentifier.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to determine context identifier. Giving up.", e);
    }
  }
}

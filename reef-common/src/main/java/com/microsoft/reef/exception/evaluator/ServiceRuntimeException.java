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
package com.microsoft.reef.exception.evaluator;


/**
 * The base class for resourcemanager exceptions thrown by REEF services, such as
 * storage and networking routines. SERVICES that throw exceptions that
 * applications may be able to cope with should subclass ServiceRuntimeException
 * or ServiceException.
 *
 * @see ServiceException which is generally preferred over ServiceRuntimeException.
 */
public class ServiceRuntimeException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private final boolean isWrappedServiceException;

  public ServiceRuntimeException() {
    this.isWrappedServiceException = false;
  }

  /**
   * It often is the case that analogous ServiceException and ServiceRuntimeExceptions
   * are needed so that exception types can be uniformly thrown from Reef APIs that
   * declare throws clauses, and legacy interfaces that do not.  This constructor
   * wraps ServiceExceptions, and is the preferred way to deal with such situations.
   *
   * @param cause
   */
  public ServiceRuntimeException(final ServiceException cause) {
    super("Wrapped ServiceException", cause);
    this.isWrappedServiceException = true;
  }

  public ServiceRuntimeException(final String message, final Throwable cause) {
    super(message, cause);
    this.isWrappedServiceException = false;
  }

  public ServiceRuntimeException(final String message) {
    super(message);
    this.isWrappedServiceException = false;

  }

  public ServiceRuntimeException(final Throwable cause) {
    super(cause);
    this.isWrappedServiceException = (cause instanceof ServiceException);
  }

  /**
   * Upon catching a ServiceRuntimeException, the receiving code should call unwrap().
   *
   * @return this, or getCause(), depending on whether or not this is a wrapped ServiceException.
   */
  public Throwable unwrap() {
    return this.isWrappedServiceException ? getCause() : this;
  }
}

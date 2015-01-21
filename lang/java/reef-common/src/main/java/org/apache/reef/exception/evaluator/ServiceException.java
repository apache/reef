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
package org.apache.reef.exception.evaluator;

/**
 * The base class for exceptions thrown by REEF libraries and services.
 * <p/>
 * Rules of thumb for exception handling in REEF:
 * <ul>
 * <li>When possible, throw a subclass of ServiceException, with the following exceptions (no pun intended)</li>
 * <li>Iterator and other standard Java interfaces neglect to declare throws
 * clauses. Use ServiceRuntimeException when implementing such interfaces.</li>
 * <li>If there is no good way for Task code to recover from the exception, throw
 * (and document) a subclass of ServiceRuntimeException</li>
 * <li>Applications with generic, catch-all error handling should catch ServiceRuntimeException and ServiceException.</li>
 * <li>Applications with specific error handling logic (eg: ignoring/coping with a failed remote task) should catch
 * the subclass of ServiceRuntimeException / ServiceException thrown by the library they are using.</li>
 * </ul>
 *
 * @see ServiceRuntimeException
 */
public class ServiceException extends Exception {
  private static final long serialVersionUID = 1L;

  public ServiceException(final String s, final Throwable e) {
    super(s, e);
  }

  public ServiceException(final String s) {
    super(s);
  }

  public ServiceException(final Throwable e) {
    super(e);
  }

}

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
package org.apache.reef.tang;

/**
 * This interface allows legacy classes to be injected by
 * ConfigurationBuilderImpl. To be of any use, implementations of this class
 * must have at least one constructor with an @Inject annotation. From
 * ConfigurationBuilderImpl's perspective, an ExternalConstructor class is just
 * a special instance of the class T, except that, after injection an
 * ExternalConstructor, ConfigurationBuilderImpl will call newInstance, and
 * store the resulting object. It will then discard the ExternalConstructor.
 *
 * @param <T> The type this ExternalConstructor will create.
 */
public interface ExternalConstructor<T> {
  /**
   * This method will only be called once.
   *
   * @return a new, distinct instance of T.
   */
  T newInstance();
}

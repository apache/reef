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
package org.apache.reef.io;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.exception.evaluator.ServiceException;

import java.util.Iterator;

/**
 * Spool files can be appended to, and can be scanned starting at the beginning.
 * Random read and write access is not supported. Spool files are ephemeral; the
 * space they use will be automatically reclaimed by REEF.
 */
@Unstable
public interface Spool<T> extends Iterable<T>, Accumulable<T> {

  /**
   * Returns an Iterable over the spool file.
   * <p/>
   * Depending on the implementation, this method may be called only once per
   * Spool instance, or it may be called repeatedly. Similarly, with some Spool
   * implementations, attempts to append to the SpoolFile after calling
   * iterator() may fail fast with a ConcurrentModificationException.
   *
   * @return An Iterator over the SpoolFile, in the order data was inserted.
   * @throws Exception
   */
  @Override
  public Iterator<T> iterator();

  /**
   * Returns an Accumulator for the spool file.
   * <p/>
   * Depending on the implementation, this method may be called only once per
   * Spool instance, or it may be called repeatedly. Similarly, with some Spool
   * implementations, attempts to append to the SpoolFile after calling
   * iterator() may fail fast with a ConcurrentModificationException.
   *
   * @throws ServiceException
   */
  @Override
  public Accumulator<T> accumulator() throws ServiceException;
}

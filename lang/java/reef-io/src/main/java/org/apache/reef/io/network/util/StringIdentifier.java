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
package org.apache.reef.io.network.util;

import org.apache.reef.wake.ComparableIdentifier;
import org.apache.reef.wake.Identifier;

/**
 * String identifier
 */
public class StringIdentifier implements ComparableIdentifier {

  private final String str;

  /**
   * Constructs a string identifier
   *
   * @param str a string
   */
  StringIdentifier(String str) {
    this.str = str;
  }

  /**
   * Returns a hash code for the object
   *
   * @return a hash code value for this object
   */
  public int hashCode() {
    return str.hashCode();
  }

  /**
   * Checks that another object is equal to this object
   *
   * @param o another object
   * @return true if the object is the same as the object argument; false, otherwise
   */
  public boolean equals(Object o) {
    return str.equals(((StringIdentifier) o).toString());
  }

  /**
   * Returns a string representation of the object
   *
   * @return a string representation of the object
   */
  public String toString() {
    return str;
  }

  @Override
  public int compareTo(Identifier o) {
    if (o == null) {
      if (str == null)
        return 0;
      return 1;
    } else {
      if (str == null)
        return -1;
      return str.compareTo(o.toString());
    }
  }
}

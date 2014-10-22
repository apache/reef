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
package com.microsoft.wake;

/*
 * An identifier class for REEF.  Identifiers are a generic naming primitive
 * that carry some information about the type of the object they point to.
 * Typical examples are server sockets, filenames, and requests.
 * 
 * Identifier constructors should take zero arguments, or take a single string.
 * 
 */
public interface Identifier {

  /**
   * Returns a hash code for the object
   * 
   * @return a hash code value for this object
   */
  public int hashCode();

  /**
   * Checks that another object is equal to this object
   * 
   * @param o
   *          another object
   * @return true if the object is the same as the object argument; false,
   *         otherwise
   */
  public boolean equals(Object o);

  /**
   * Return a string representation of this object. This method should return a
   * URL-style string, that begins with "type://", where "type" is chosen to
   * uniquely identify this type of identifier.
   */
  public String toString();

}

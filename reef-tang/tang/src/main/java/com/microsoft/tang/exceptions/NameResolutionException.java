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
package com.microsoft.tang.exceptions;

/**
 * Thrown when Tang encounters an unknown (to the current classloader) class
 * or configuration option.  NameResolutionExceptions can only be encountered
 * if:
 * <ol>
 * <li> Tang is processing a configuration file from an external source </li>
 * <li> Classes / NamedParameters are passed to Tang in String form </li>
 * <li> Class objects are passed directly to Tang, but it is using a different
 *      classloader than the calling code.</li>
 * <li> Tang is processing Configurations using a ClassHierarchy produced by
 *      another process </li> 
 * </ol>
 */
public class NameResolutionException extends BindException {
  private static final long serialVersionUID = 1L;

  public NameResolutionException(String name, String longestPrefix) {
      super("Could not resolve " + name + ".  Search ended at prefix " + longestPrefix + " This can happen due to typos in class names that are passed as strings, or because Tang is configured to use a classloader other than the one that generated the class reference (check your classpath and the code that instantiated Tang)");
  }
}

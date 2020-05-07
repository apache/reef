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
package org.apache.reef.tang.formats;

import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.exceptions.BindException;

import java.io.File;
import java.io.IOException;

/**
 * A base interface for Configuration serializers.
 */
@DefaultImplementation(AvroConfigurationSerializer.class)
public interface ConfigurationSerializer {


  /**
   * Stores the given Configuration in the given File.
   *
   * @param conf the Configuration to store
   * @param file the file to store the Configuration in
   * @throws java.io.IOException if there is an IO error in the process.
   */
  void toFile(Configuration conf, File file) throws IOException;

  /**
   * Stores the given Configuration in the given Text File.
   *
   * @param conf the Configuration to store
   * @param file the file to store the Configuration in
   * @throws java.io.IOException if there is an IO error in the process.
   */
  void toTextFile(Configuration conf, File file) throws IOException;

  /**
   * Writes the Configuration to a byte[].
   *
   * @param conf the Configuration to be converted
   * @return the byte array
   * @throws IOException if encoding fails to write
   */
  byte[] toByteArray(Configuration conf) throws IOException;

  /**
   * Writes the Configuration as a String.
   *
   * @param configuration the Configuration to be converted
   * @return a String representation of the Configuration
   */
  String toString(Configuration configuration);


  /**
   * Loads a Configuration from a File created with toFile().
   *
   * @param file the File to read from.
   * @return the Configuration stored in the file.
   * @throws IOException   if the File can't be read or parsed
   * @throws BindException if the file contains an illegal Configuration
   */
  Configuration fromFile(File file) throws IOException, BindException;

  /**
   * Loads a Configuration from a File created with toFile().
   *
   * @param file the File to read from.
   * @return the Configuration stored in the file.
   * @throws IOException   if the File can't be read or parsed
   * @throws BindException if the file contains an illegal Configuration
   */
  Configuration fromTextFile(File file) throws IOException, BindException;

  /**
   * Loads a Configuration from a File created with toFile() with ClassHierarchy.
   *
   * @param file the File to read from.
   * @param classHierarchy the class hierarchy to be used.
   * @return the Configuration stored in the file.
   * @throws IOException if the File can't be read or parsed
   */
  Configuration fromTextFile(File file, ClassHierarchy classHierarchy) throws IOException;

  /**
   * Loads a Configuration from a File created with toFile().
   *
   * @param file           the File to read from.
   * @param classHierarchy used to validate the configuration against
   * @return the Configuration stored in the file.
   * @throws IOException   if the File can't be read or parsed
   * @throws BindException if the file contains an illegal Configuration
   */
  Configuration fromFile(File file, ClassHierarchy classHierarchy) throws IOException, BindException;

  /**
   * Loads a Configuration from a byte[] created with toByteArray().
   *
   * @param theBytes the bytes to deserialize.
   * @return the Configuration stored.
   * @throws IOException   if the byte[] can't be deserialized
   * @throws BindException if the byte[] contains an illegal Configuration.
   */
  Configuration fromByteArray(byte[] theBytes) throws IOException, BindException;

  /**
   * Loads a Configuration from a byte[] created with toByteArray().
   *
   * @param theBytes       the bytes to deserialize.
   * @param classHierarchy used to validate the configuration against
   * @return the Configuration stored.
   * @throws IOException   if the byte[] can't be deserialized
   * @throws BindException if the byte[] contains an illegal Configuration.
   */
  Configuration fromByteArray(byte[] theBytes, ClassHierarchy classHierarchy)
      throws IOException, BindException;

  /**
   * Decodes a String generated via toString().
   *
   * @param theString to be parsed
   * @return the Configuration stored in theString.
   * @throws IOException   if theString can't be parsed.
   * @throws BindException if theString contains an illegal Configuration.
   */
  Configuration fromString(String theString) throws IOException, BindException;

  /**
   * Decodes a String generated via toString().
   *
   * @param theString      to be parsed
   * @param classHierarchy used to validate the configuration against
   * @return the Configuration stored in theString.
   * @throws IOException   if theString can't be parsed.
   * @throws BindException if theString contains an illegal Configuration.
   */
  Configuration fromString(String theString, ClassHierarchy classHierarchy)
      throws IOException, BindException;
}

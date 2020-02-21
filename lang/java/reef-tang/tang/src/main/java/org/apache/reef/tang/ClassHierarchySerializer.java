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

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.implementation.avro.AvroClassHierarchySerializer;

import java.io.*;

/**
 * A base interface for ClassHierarchy serializers.
 */
@DefaultImplementation(AvroClassHierarchySerializer.class)
public interface ClassHierarchySerializer {
  /**
   * Writes a ClassHierarchy into a file.
   *
   * @param classHierarchy the ClassHierarchy to store
   * @param file the file to store the ClassHierarchy
   * @throws IOException if there is an error in the process
   */
  void toFile(ClassHierarchy classHierarchy, File file) throws IOException;

  /**
   * Writes a ClassHierarchy into a text file.
   *
   * @param classHierarchy the ClassHierarchy to store
   * @param file the text file to store the ClassHierarchy
   * @throws IOException if there is an error in the process
   */
  void toTextFile(ClassHierarchy classHierarchy, File file) throws IOException;

   /**
   * Serializes a ClassHierarchy as a byte[].
   *
   * @param classHierarchy the ClassHierarchy to store
   * @return the byte array containing the serialized class hierarchy
   * @throws IOException if there is an error in the process
   */
  byte[] toByteArray(ClassHierarchy classHierarchy) throws IOException;

  /**
   * Serializes a ClassHierarchy as a String.
   *
   * @param classHierarchy the ClassHierarchy to store
   * @return the string containing the serialized class hierarchy
   * @throws IOException if there is an error in the process
   */
  String toString(ClassHierarchy classHierarchy) throws IOException;

  /**
   * Loads a ClassHierarchy from a file created with toFile().
   *
   * @param file the File to read from
   * @return the class hierarchy
   * @throws IOException if the File can't be read or parsed
   */
  ClassHierarchy fromFile(File file) throws IOException;

  /**
   * Loads a ClassHierarchy from a text file created with toTextFile().
   *
   * @param file the File to read from
   * @return the class hierarchy
   * @throws IOException if the File can't be read or parsed
   */
  ClassHierarchy fromTextFile(File file) throws IOException;

  /**
   * Deserializes a ClassHierarchy from a byte[] created with toByteArray().
   *
   * @param theBytes the byte[] to deserialize
   * @return the class hierarchy
   * @throws IOException if the byte[] can't be read or parsed
   */
  ClassHierarchy fromByteArray(byte[] theBytes) throws IOException;

  /**
   * Deserializes a ClassHierarchy from a String created with toString().
   *
   * @param theString the String to deserialize
   * @return the class hierarchy
   * @throws IOException if the String can't be read or parsed
   */
  ClassHierarchy fromString(String theString) throws IOException;
}

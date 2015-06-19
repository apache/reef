package org.apache.reef.tang.formats;

import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.formats.avro.AvroNode;

import java.io.*;

/**
 * A base interface for ClassHierarchy serializers.
 */
@DefaultImplementation(org.apache.reef.tang.formats.AvroClassHierarchySerializer.class)
public interface ClassHierarchySerializer {
  /**
   * Writes a ClassHierarchy into a file.
   *
   * @param classHierarchy the ClassHierarchy to store
   * @param file the file to store the ClassHierarchy
   * @throws IOException if there is an error in the process
   */
  void toFile(final ClassHierarchy classHierarchy, final File file) throws IOException;

  /**
   * Writes a ClassHierarchy into a text file.
   *
   * @param classHierarchy the ClassHierarchy to store
   * @param file the text file to store the ClassHierarchy
   * @throws IOException if there is an error in the process
   */
  void toTextFile(final ClassHierarchy classHierarchy, final File file) throws IOException;

   /**
   * Serializes a ClassHierarchy as a byte[].
   *
   * @param classHierarchy the ClassHierarchy to store
   * @throws IOException if there is an error in the process
   */
  byte[] toByteArray(final ClassHierarchy classHierarchy) throws IOException;

  /**
   * Serializes a ClassHierarchy as a String.
   *
   * @param classHierarchy the ClassHierarchy to store
   * @throws IOException if there is an error in the process
   */
  String toString(final ClassHierarchy classHierarchy) throws IOException;

  /**
   * Loads a ClassHierarchy from a file created with toFile().
   *
   * @param file the File to read from
   * @throws IOException if the File can't be read or parsed
   */
  ClassHierarchy fromFile(final File file) throws IOException;

  /**
   * Loads a ClassHierarchy from a text file created with toTextFile().
   *
   * @param file the File to read from
   * @throws IOException if the File can't be read or parsed
   */
  ClassHierarchy fromTextFile(final File file) throws IOException;

  /**
   * Deserializes a ClassHierarchy from a byte[] created with toByteArray().
   *
   * @param theBytes the byte[] to deserialize
   * @throws IOException if the byte[] can't be read or parsed
   */
  ClassHierarchy fromByteArray(final byte[] theBytes) throws IOException;

  /**
   * Deserializes a ClassHierarchy from a String created with toString().
   *
   * @param theString the String to deserialize
   * @throws IOException if the String can't be read or parsed
   */
  ClassHierarchy fromString(final String theString) throws IOException;
}

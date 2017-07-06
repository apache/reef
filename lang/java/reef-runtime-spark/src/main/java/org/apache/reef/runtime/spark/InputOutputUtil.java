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
package org.apache.reef.runtime.spark;


import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.LineReader;

/**
 * This class provides convenient methods for accessing
 * some Input/Output methods.
 */
final class InputOutputUtil{
  private static Logger logger=Logger.getLogger(InputOutputUtil.class.getName());

  public static void close(final LineReader reader){
    if (reader==null) {
      return;
    }
    //
    try {
      reader.close();
    } catch (Exception ignore) {
      logger.info("InputOutputUtil::close LineReader in catch block with message="+ignore.getMessage());
    }
  }

  public static void close(final OutputStream stream){
    if (stream==null) {
      return;
    }
    //
    try {
      stream.close();
    } catch (Exception ignore) {
      logger.info("InputOutputUtil::in stream catch block with message="+ignore.getMessage());
    }
  }

  public static void close(final InputStream stream){
    if (stream==null) {
      return;
    }
    //
    try {
      stream.close();
    } catch (Exception ignore) {
      logger.info("InputOutputUtil::in stream catch block with message="+ignore.getMessage());
    }
  }

  public static void close(final FSDataInputStream stream){
    if (stream==null) {
      return;
    }
    //
    try {
      stream.close();
    } catch (Exception ignore) {
      logger.info("InputOutputUtil::close in catch block with message="+ignore.getMessage());
    }
  }

  public static void close(final BufferedReader reader){
    if (reader==null) {
      return;
    }
    //
    try {
      reader.close();
    } catch (Exception ignore) {
      logger.info("InputOutputUtil::close reader in catch block with message="+ignore.getMessage());
    }
  }

  private InputOutputUtil(){
  }

}

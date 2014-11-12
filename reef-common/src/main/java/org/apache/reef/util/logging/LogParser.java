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

package org.apache.reef.util.logging;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Parse logs for reporting
 */
public class LogParser {

  public static String endIndicators[] = {
      LoggingScopeImpl.EXIT_PREFIX + LoggingScopeFactory.BRIDGE_SETUP,
      LoggingScopeImpl.EXIT_PREFIX + LoggingScopeFactory.EVALUATOR_SUBMIT,
      LoggingScopeImpl.EXIT_PREFIX + LoggingScopeFactory.EVALUATOR_BRIDGE_SUBMIT,
      LoggingScopeImpl.EXIT_PREFIX + LoggingScopeFactory.DRIVER_START,
      LoggingScopeImpl.EXIT_PREFIX + LoggingScopeFactory.EVALUATOR_LAUNCH,
      LoggingScopeImpl.EXIT_PREFIX + LoggingScopeFactory.EVALUATOR_ALLOCATED,
      LoggingScopeImpl.EXIT_PREFIX + LoggingScopeFactory.ACTIVE_CONTEXT,
      LoggingScopeImpl.EXIT_PREFIX + LoggingScopeFactory.HTTP_REQUEST,
      LoggingScopeImpl.EXIT_PREFIX + LoggingScopeFactory.TASK_COMPLETE
  };

  public static String startIndicators[] = {
      LoggingScopeImpl.START_PREFIX + LoggingScopeFactory.DRIVER_START,
      LoggingScopeImpl.START_PREFIX + LoggingScopeFactory.BRIDGE_SETUP,
      LoggingScopeImpl.START_PREFIX + LoggingScopeFactory.EVALUATOR_BRIDGE_SUBMIT,
      LoggingScopeImpl.START_PREFIX + LoggingScopeFactory.EVALUATOR_SUBMIT,
      LoggingScopeImpl.START_PREFIX + LoggingScopeFactory.EVALUATOR_ALLOCATED,
      LoggingScopeImpl.START_PREFIX + LoggingScopeFactory.EVALUATOR_LAUNCH,
      LoggingScopeImpl.START_PREFIX + LoggingScopeFactory.ACTIVE_CONTEXT,
      LoggingScopeImpl.START_PREFIX + LoggingScopeFactory.HTTP_REQUEST,
      LoggingScopeImpl.START_PREFIX + LoggingScopeFactory.TASK_COMPLETE
  };

  private LogParser() {
  }

  /**
   * Get lines from a given file with a specified filter, trim the line by removing strings before removeBeforeToken and after removeAfterToken
   * @param fileName
   * @param filter
   * @return
   * @throws IOException
   */
  public static ArrayList<String> getFilteredLinesFromFile(final String fileName, final String filter, final String removeBeforeToken, final String removeAfterToken) throws IOException{
    final ArrayList<String> filteredLines = new ArrayList<String>();
    try (final FileReader fr =  new FileReader(fileName)) {
      try (final BufferedReader in = new BufferedReader(fr)) {
        String line = "";
        while ((line = in.readLine()) != null) {
          if (line.trim().length() == 0) {
            continue;
          }
          if (line.contains(filter)) {
            String trimedLine;
            if (removeBeforeToken != null) {
              final String[] p = line.split(removeBeforeToken);
              if (p.length > 1) {
                trimedLine = p[p.length-1];
              } else {
                trimedLine = line.trim();
              }
            } else {
              trimedLine = line.trim();
            }
            if (removeAfterToken != null) {
              final String[] p = trimedLine.split(removeAfterToken);
              if (p.length > 1) {
                trimedLine = p[0];
              }
            }
            filteredLines.add(trimedLine);
          }
        }
      }
    }
    return filteredLines;
  }

  /**
   * get lines from given file with specified filter
   * @param fileName
   * @param filter
   * @return
   * @throws IOException
   */
  public static ArrayList<String> getFilteredLinesFromFile(final String fileName, final String filter) throws IOException {
    return getFilteredLinesFromFile(fileName, filter, null, null);
  }

  /**
   * filter array list of lines and get the last portion of the line separated by the token, like ":::"
   * @param original
   * @param filter
   * @return
   */
  public static ArrayList<String> filter(final ArrayList<String> original, final String filter, final String token) {
    final ArrayList<String> result = new ArrayList<String>();
    for (String line : original) {
      if (line.contains(filter)) {
        final String[] p = line.split(token);
        if (p.length > 1) {
          result.add(p[p.length-1]);
        }
      }
    }
    return result;
  }

  /**
   * find lines that contain stage indicators. The stageIndicators must be in sequence which appear in the lines.
   * @param lines
   * @param stageIndicators
   * @return
   */
  public static ArrayList<String> findStages(final ArrayList<String> lines, final String[] stageIndicators) {
    ArrayList<String> stages = new ArrayList<String>();

    int i = 0;
    for (String line: lines) {
      if (line.contains(stageIndicators[i])){
        stages.add(stageIndicators[i]);
        if (i < stageIndicators.length - 1) {
          i++;
        }
      }
    }
    return stages;
  }

  public static ArrayList<String> mergeStages(ArrayList<String> startStages, ArrayList<String> endStages) {
    ArrayList<String> mergeStage = new ArrayList<String>();
    for (int i = 0; i < startStages.size(); i++) {
      String end = startStages.get(i).replace(LoggingScopeImpl.START_PREFIX, LoggingScopeImpl.EXIT_PREFIX);
      if (endStages.contains(end)) {
        mergeStage.add(startStages.get(i)  + "   " + end);
      } else {
        mergeStage.add(startStages.get(i));
      }
    }
    return mergeStage;
  }
}
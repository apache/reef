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
package org.apache.reef.examples.group.bgd.data.parser;

import org.apache.commons.lang.StringUtils;
import org.apache.reef.examples.group.bgd.data.Example;
import org.apache.reef.examples.group.bgd.data.SparseExample;

import javax.inject.Inject;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Parser for SVMLight records.
 */
public class SVMLightParser implements Parser<String> {

  private static final Logger LOG = Logger.getLogger(SVMLightParser.class.getName());

  @Inject
  public SVMLightParser() {
  }

  @Override
  public Example parse(final String line) {

    final int entriesCount = StringUtils.countMatches(line, ":");
    final int[] indices = new int[entriesCount];
    final float[] values = new float[entriesCount];

    final String[] entries = StringUtils.split(line, ' ');
    String labelStr = entries[0];

    final boolean pipeExists = labelStr.indexOf('|') != -1;
    if (pipeExists) {
      labelStr = labelStr.substring(0, labelStr.indexOf('|'));
    }
    double label = Double.parseDouble(labelStr);

    if (label != 1) {
      label = -1;
    }

    for (int j = 1; j < entries.length; ++j) {
      final String x = entries[j];
      final String[] entity = StringUtils.split(x, ':');
      final int offset = pipeExists ? 0 : 1;
      indices[j - 1] = Integer.parseInt(entity[0]) - offset;
      values[j - 1] = Float.parseFloat(entity[1]);
    }
    return new SparseExample(label, values, indices);
  }

  public static void main(final String[] args) {
    final Parser<String> parser = new SVMLightParser();
    for (int i = 0; i < 10; i++) {
      final List<SparseExample> examples = new ArrayList<>();
      float avgFtLen = 0;
      try (BufferedReader br = new BufferedReader(new InputStreamReader(
              new FileInputStream("C:\\Users\\shravan\\data\\splice\\hdi\\hdi_uncomp\\part-r-0000" + i),
              StandardCharsets.UTF_8))) {
        String line;
        while ((line = br.readLine()) != null) {
          final SparseExample spEx = (SparseExample) parser.parse(line);
          avgFtLen += spEx.getFeatureLength();
          examples.add(spEx);
        }
      } catch (final IOException e) {
        throw new RuntimeException("Exception", e);
      }

      LOG.log(Level.INFO, "OUT: {0} {1} {2}",
          new Object[] {examples.size(), avgFtLen, avgFtLen / examples.size()});

      examples.clear();
    }
  }
}

package com.microsoft.reef.examples.data.loading;

import org.apache.commons.lang.StringUtils;

import javax.inject.Inject;

/**
 * A Parser for SVMLight records
 */
public class SVMLightParser implements Parser<String> {

  @Inject
  public SVMLightParser() {
  }

  @Override
  public Example parse(final String line) {
    final int entriesCount = StringUtils.countMatches(line, ":");
    final int[] indices = new int[entriesCount];
    final double[] values = new double[entriesCount];

    final String[] entries = StringUtils.split(line, ' ');
    double label = Double.parseDouble(entries[0]);

    if (label != 1) {
      label = -1;
    }

    for (int j = 1; j < entries.length; ++j) {
      final String x = entries[j];
      final String[] entity = StringUtils.split(x, ':');
      indices[j - 1] = Integer.parseInt(entity[0]) - 1;
      values[j - 1] = Double.parseDouble(entity[1]);
    }
    return new SparseExample(label, values, indices);
  }
}

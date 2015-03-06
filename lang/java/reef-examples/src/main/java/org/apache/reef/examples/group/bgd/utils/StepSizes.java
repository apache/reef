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
package org.apache.reef.examples.group.bgd.utils;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StepSizes {

  private static final Logger LOG = Logger.getLogger(StepSizes.class.getName());

  private final double[] t;
  private final int gridSize = 21;

  @Inject
  public StepSizes() {
    this.t = new double[gridSize];
    final int mid = (gridSize / 2);
    t[mid] = 1;
    for (int i = mid - 1; i >= 0; i--) {
      t[i] = t[i + 1] / 2.0;
    }
    for (int i = mid + 1; i < gridSize; i++) {
      t[i] = t[i - 1] * 2.0;
    }
  }

  public double[] getT() {
    return t;
  }

  public int getGridSize() {
    return gridSize;
  }

  public static void main(final String[] args) {
    // TODO Auto-generated method stub
    final StepSizes t = new StepSizes();
    LOG.log(Level.INFO, "OUT: {0}", Arrays.toString(t.getT()));
  }
}

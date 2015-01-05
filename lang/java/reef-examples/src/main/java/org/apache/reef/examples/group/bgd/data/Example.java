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
package org.apache.reef.examples.group.bgd.data;

import org.apache.reef.examples.group.utils.math.Vector;

import java.io.Serializable;

/**
 * Base interface for Examples for linear models.
 */
public interface Example extends Serializable {

  /**
   * Access to the label.
   *
   * @return the label
   */
  double getLabel();

  /**
   * Computes the prediction for this Example, given the model w.
   * <p/>
   * w.dot(this.getFeatures())
   *
   * @param w the model
   * @return the prediction for this Example, given the model w.
   */
  double predict(Vector w);

  /**
   * Adds the current example's gradient to the gradientVector, assuming that
   * the gradient with respect to the prediction is gradient.
   */
  void addGradient(Vector gradientVector, double gradient);
}

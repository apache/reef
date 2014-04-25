/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.examples.data.loading;

import com.microsoft.canberra.math.Vector;

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
  public abstract double getLabel();

  /**
   * Computes the prediction for this Example, given the model w.
   * <p/>
   * w.dot(this.getFeatures())
   *
   * @param w the model
   * @return the prediction for this Example, given the model w.
   */
  public abstract double predict(Vector w);

  /**
   * Adds the current example's gradient to the gradientVector, assuming that
   * the gradient with respect to the prediction is gradient.
   *
   * @param gradientVector
   * @param gradient
   */
  public abstract void addGradient(Vector gradientVector, double gradient);

}
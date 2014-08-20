/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.reef.io.network.group.impl.config;

import com.microsoft.reef.io.network.group.config.OP_TYPE;
import com.microsoft.wake.remote.Codec;

/**
 * Base class for description of group communication operators
 */
public class GroupOperatorDescription {
  /** Type of the operator */
  public final OP_TYPE operatorType;

  /** Codec to be used to serialize data */
  public final Class<? extends Codec<?>> dataCodecClass;

  /**
   * Constructor for fields
   * 
   * @param operatorType
   * @param dataCodecClass
   */
  public GroupOperatorDescription(OP_TYPE operatorType,
      Class<? extends Codec<?>> dataCodecClass) {
    super();
    this.operatorType = operatorType;
    this.dataCodecClass = dataCodecClass;
  }
}

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
package com.microsoft.reef.io.network.nggroup.api;

import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Name;

/**
 *
 */
public interface CommunicationGroupDriver {

  /**
   * @param string
   * @param dataCodec
   * @return
   */
  public CommunicationGroupDriver addBroadcast(
      Class<? extends Name<String>> operatorName, BroadcastOperatorSpec spec);

  /**
   * @param string
   * @param dataCodec
   * @param reduceFunction
   * @return
   */
  public CommunicationGroupDriver addReduce(
      Class<? extends Name<String>> operatorName, ReduceOperatorSpec spec);

  /**
   *
   */
  public void finalise();

  /**
   * @param build
   * @return
   */
  public Configuration getConfiguration(Configuration taskConf);

  /**
   * @param partialTaskConf
   */
  public void addTask(Configuration partialTaskConf);

}
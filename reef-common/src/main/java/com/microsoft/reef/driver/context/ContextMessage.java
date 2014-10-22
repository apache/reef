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
package com.microsoft.reef.driver.context;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.io.Message;
import com.microsoft.reef.io.naming.Identifiable;

/**
 * Driver-side representation of a message sent by a Context to the Driver.
 */
@Public
@DriverSide
@Provided
public interface ContextMessage extends Message, Identifiable {

  /**
   * @return the message sent by the Context.
   */
  @Override
  public byte[] get();

  /**
   * @return the ID of the sending Context.
   */
  @Override
  public String getId();

  /**
   * @return the ID of the ContextMessageSource that sent the message on the Context.
   */
  public String getMessageSourceID();

}

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
package com.microsoft.reef.client;

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.util.RuntimeError;
import com.microsoft.tang.annotations.DefaultImplementation;

/**
 * Receives fatal runtime errors of REEF.
 * <p/>
 * This interface needs to be implemented by a client of REEF and bound as part
 * of constructing a REEF instance.
 */
@Public
@ClientSide
@Deprecated
@DefaultImplementation(DefaultRuntimeErrorHandler.class)
public interface RuntimeErrorHandler {

  /**
   * Receives fatal runtime errors. The presence of this error means that the
   * underlying REEF instance is no longer able to execute REEF jobs. The
   * actual Jobs may or may not still be running.
   *
   * @param error
   */
  public void onError(final RuntimeError error);
}

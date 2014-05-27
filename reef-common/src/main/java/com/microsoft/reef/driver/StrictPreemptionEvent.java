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
package com.microsoft.reef.driver;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.Unstable;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;

/**
 * Represents a strict preemption event: It contains the set of Evaluators that the underlying resource manager will
 * take away from the Driver.
 * <p/>
 * NOTE: This currently not implemented. Consider it a preview of the API.
 */
@Unstable
@DriverSide
@Public
@Provided
public interface StrictPreemptionEvent extends PreemptionEvent {
}

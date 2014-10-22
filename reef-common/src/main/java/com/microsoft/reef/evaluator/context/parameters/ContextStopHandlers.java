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
package com.microsoft.reef.evaluator.context.parameters;

import com.microsoft.reef.evaluator.context.events.ContextStop;
import com.microsoft.reef.runtime.common.evaluator.context.defaults.DefaultContextStopHandler;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.wake.EventHandler;

import java.util.Set;

/**
* The set of event handlers for the ContextStop event.
*/
@NamedParameter(doc = "The set of event handlers for the ContextStop event.",
    default_classes = DefaultContextStopHandler.class)
public final class ContextStopHandlers implements Name<Set<EventHandler<ContextStop>>> {
}

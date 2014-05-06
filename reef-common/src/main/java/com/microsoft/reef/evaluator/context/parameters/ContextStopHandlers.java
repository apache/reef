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

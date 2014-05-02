package com.microsoft.reef.evaluator.context.parameters;

import com.microsoft.reef.evaluator.context.events.ContextStart;
import com.microsoft.reef.runtime.common.evaluator.context.defaults.DefaultContextStartHandler;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.wake.EventHandler;

import java.util.Set;

/**
* The set of event handlers for the ContextStart event.
*/
@NamedParameter(doc = "The set of event handlers for the ContextStart event.",
    default_classes = DefaultContextStartHandler.class)
public class ContextStartHandlers implements Name<Set<EventHandler<ContextStart>>> {
}

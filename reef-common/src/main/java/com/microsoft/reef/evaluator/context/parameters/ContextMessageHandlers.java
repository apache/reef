package com.microsoft.reef.evaluator.context.parameters;

import com.microsoft.reef.evaluator.context.ContextMessageHandler;
import com.microsoft.reef.runtime.common.evaluator.context.defaults.DefaultContextMessageHandler;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

import java.util.Set;

/**
 * The set of Context message handlers.
 */
@NamedParameter(doc = "The set of Context message handlers.",
    default_classes = DefaultContextMessageHandler.class)
public final class ContextMessageHandlers implements Name<Set<ContextMessageHandler>> {
}

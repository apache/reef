package com.microsoft.reef.evaluator.context.parameters;

import com.microsoft.reef.evaluator.context.ContextMessageSource;
import com.microsoft.reef.runtime.common.evaluator.context.defaults.DefaultContextMessageSource;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

import java.util.Set;

/**
* The set of ContextMessageSource implementations called during heartbeats.
*/
@NamedParameter(doc = "The set of ContextMessageSource implementations called during heartbeats.",
    default_classes = DefaultContextMessageSource.class)
public final class ContextMessageSources implements Name<Set<ContextMessageSource>> {
}

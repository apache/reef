package com.microsoft.reef.evaluator.context.parameters;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.util.ObjectInstantiationLogger;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

import java.util.Set;

/**
 * A set of classes to be instantiated and shared as singletons within this context and all child context
 */
@NamedParameter(doc = "A set of classes to be instantiated and shared as singletons within this context and all child context",
    default_classes = ObjectInstantiationLogger.class)
@Private
public class Services implements Name<Set<Object>> {
}

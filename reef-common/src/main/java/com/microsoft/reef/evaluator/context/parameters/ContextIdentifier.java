package com.microsoft.reef.evaluator.context.parameters;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
* Context identifier.
*/
@NamedParameter(doc = "The identifier for the context.")
public final class ContextIdentifier implements Name<String> {
}

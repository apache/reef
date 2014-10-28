package org.apache.reef.examples.library;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Command line parameter: a command to run. e.g. "echo Hello REEF"
 */
@NamedParameter(doc = "The shell command", short_name = "cmd", default_value = "*INTERACTIVE*")
public final class Command implements Name<String> {
}
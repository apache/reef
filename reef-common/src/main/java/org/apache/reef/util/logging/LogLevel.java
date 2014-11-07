package org.apache.reef.util.logging;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import java.util.logging.Level;

/**
 * Log Level named parameter for LoggingScopeFactory
 */
@NamedParameter(default_class = Level.class)
public class LogLevel implements Name<Level>
{
}

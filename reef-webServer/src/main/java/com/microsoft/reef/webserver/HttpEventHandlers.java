package com.microsoft.reef.webserver;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

import java.util.Set;

/**
 * HttpEventHandlers
 */
@NamedParameter(doc="Http Event Handlers")
public class HttpEventHandlers implements Name<Set<HttpHandler>>
{
}

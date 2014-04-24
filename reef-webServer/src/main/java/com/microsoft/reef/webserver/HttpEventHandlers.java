package com.microsoft.reef.webserver;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

import java.util.Set;

/**
 * HttpEventHandlers. A NamedParameter used in HttpServerImpl
 */

@NamedParameter
public class HttpEventHandlers implements Name<Set<HttpHandler>>
{
}

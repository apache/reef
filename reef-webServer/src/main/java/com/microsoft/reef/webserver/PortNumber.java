package com.microsoft.reef.webserver;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * port number for teh Http Server
 */
@NamedParameter(default_value = "8080")
class PortNumber implements Name<String>
{}

/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.webserver;

import com.microsoft.reef.runtime.yarn.driver.TrackingURLProvider;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;

/**
 * Configuration Module Builder for Http Handler
 */
public final class HttpHandlerConfiguration extends ConfigurationModuleBuilder {

  /**
   * Specify optional parameter for HttpEventHandlers
   */
  public static final OptionalParameter<HttpHandler> HTTP_HANDLERS = new OptionalParameter<>();

  /**
   * HttpHandlerConfiguration merged with HttpRuntimeConfiguration
   */
  public static final ConfigurationModule CONF = new HttpHandlerConfiguration().merge(HttpRuntimeConfiguration.CONF)
      .bindSetEntry(HttpEventHandlers.class, HTTP_HANDLERS)
      .bindImplementation(TrackingURLProvider.class, HttpTrackingURLProvider.class)
      .build();
}

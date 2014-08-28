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
package com.microsoft.reef.io.network.naming;

import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.wake.IdentifierFactory;

public class NameServerParameters {

  @NamedParameter(doc = "port for the name service", default_value = "0", short_name = "nameport")
  public class NameServerPort implements Name<Integer> {
  }

  @NamedParameter(doc = "DNS hostname running the name service")
  public class NameServerAddr implements Name<String> {
  }

  @NamedParameter(doc = "identifier factory for the name service", default_class = StringIdentifierFactory.class)
  public class NameServerIdentifierFactory implements Name<IdentifierFactory> {
  }

}

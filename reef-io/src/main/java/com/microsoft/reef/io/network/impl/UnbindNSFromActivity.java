/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.impl;

import javax.inject.Inject;

import com.microsoft.reef.activity.events.ActivityStop;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.IdentifierFactory;

/**
 * 
 */
public class UnbindNSFromActivity implements EventHandler<ActivityStop>{
  private final NetworkService<?> ns;
  private final IdentifierFactory idFac;
  
  /**
   * 
   */
  @Inject
  public UnbindNSFromActivity(
		  NetworkService<?> ns, 
		  @Parameter(NetworkServiceParameters.NetworkServiceIdentifierFactory.class) IdentifierFactory idFac) {
    this.ns = ns;
    this.idFac = idFac;
  }

  @Override
  public void onNext(ActivityStop activity) {
    ns.unregisterId(idFac.getNewInstance(activity.getId()));
  }

}

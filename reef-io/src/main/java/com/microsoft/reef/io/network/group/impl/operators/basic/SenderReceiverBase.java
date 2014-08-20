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
package com.microsoft.reef.io.network.group.impl.operators.basic;

import java.util.Collections;
import java.util.List;

import com.microsoft.reef.io.network.group.operators.AbstractGroupCommOperator;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Gather;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.group.operators.Scatter;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.Identifier;

/**
 * Base class for all asymmetric operators
 * {@link Scatter}, {@link Broadcast}, {@link Gather}, {@link Reduce}
 */
public class SenderReceiverBase extends AbstractGroupCommOperator{

  private Identifier self;
  private Identifier parent;
  private List<ComparableIdentifier> children;

  public SenderReceiverBase() {
    super();
  }

  public SenderReceiverBase(final Identifier self, final Identifier parent,
                            final List<ComparableIdentifier> children) {
    super();
    this.setSelf(self);
    this.setParent(parent);
    this.setChildren(children);
    if (children != null) {
      Collections.sort(children);
    }
  }

  public Identifier getParent() {
    return parent;
  }

  public void setParent(final Identifier parent) {
    this.parent = parent;
  }

  public Identifier getSelf() {
    return self;
  }

  public void setSelf(final Identifier self) {
    this.self = self;
  }

  public List<ComparableIdentifier> getChildren() {
    return children;
  }

  public void setChildren(final List<ComparableIdentifier> children) {
    this.children = children;
  }

}
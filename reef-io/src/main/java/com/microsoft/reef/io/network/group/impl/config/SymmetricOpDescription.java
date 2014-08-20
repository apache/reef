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
package com.microsoft.reef.io.network.group.impl.config;

import com.microsoft.reef.io.network.group.config.OP_TYPE;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.remote.Codec;

import java.util.List;

/**
 * This is a type of {@link GroupOperatorDescription} and the base class for
 * descriptions of all symmetric operators. We do not distinguish between
 * operators that need a Reduce and those that do not because only AllGather
 * does not need Reduce while both ReduceScatter & AllReduce need it.
 */
public class SymmetricOpDescription extends GroupOperatorDescription {
  /** Tasks participating in this group. */
  public final List<ComparableIdentifier> taskIds;

  /** The reduce function to be used for operations that do reduction */
  public final Class<? extends ReduceFunction<?>> redFuncClass;

  /**
   * Constructor for fields
   * 
   * @param operatorType
   * @param dataCodecClass
   * @param taskIds
   * @param redFuncClass
   */
  public SymmetricOpDescription(OP_TYPE operatorType,
      Class<? extends Codec<?>> dataCodecClass,
      List<ComparableIdentifier> taskIds,
      Class<? extends ReduceFunction<?>> redFuncClass) {
    super(operatorType, dataCodecClass);
    this.taskIds = taskIds;
    this.redFuncClass = redFuncClass;
  }
  
  /** Builder for fluent description of operators */
  public static class Builder implements com.microsoft.reef.util.Builder<SymmetricOpDescription>{

    private OP_TYPE operatorType;
    private Class<? extends Codec<?>> dataCodecClass;
    private Class<? extends ReduceFunction<?>> redFuncClass;
    private List<ComparableIdentifier> tasks;
    
    /**
     * Override the operator type which is typically automatically set
     * 
     * @param operatorType
     * @return
     */
    public Builder setOpertaorType(OP_TYPE operatorType) {
      this.operatorType = operatorType;
      return this;
    }

    /**
     * Set the Data Codec class typically inherited from GroupOperators
     * 
     * @param dataCodecClass
     * @return
     */
    public Builder setDataCodecClass(Class<? extends Codec<?>> dataCodecClass) {
      this.dataCodecClass = dataCodecClass;
      return this;
    }

    /**
     * Set the list of ids of senders
     * 
     * @param tasks
     * @return
     */
    public Builder setTasks(List<ComparableIdentifier> tasks) {
      this.tasks = tasks;
      return this;
    }


    /**
     * Set the reduce function class to be used
     * 
     * @param redFuncClass
     * @return
     */
    public Builder setRedFuncClass(
        Class<? extends ReduceFunction<?>> redFuncClass) {
      this.redFuncClass = redFuncClass;
      return this;
    }


    /* Build the operator description
     * @see org.apache.reef.utils.Builder#build()
     */
    @Override
    public SymmetricOpDescription build() {
      return new SymmetricOpDescription(operatorType, dataCodecClass, tasks, redFuncClass);
    }
    
  }

}

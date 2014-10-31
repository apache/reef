/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.tang.implementation.protobuf;

import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.implementation.Constructor;
import org.apache.reef.tang.implementation.InjectionPlan;
import org.apache.reef.tang.implementation.Subplan;
import org.apache.reef.tang.implementation.java.JavaInstance;
import org.apache.reef.tang.proto.InjectionPlanProto;
import org.apache.reef.tang.types.ClassNode;
import org.apache.reef.tang.types.ConstructorDef;
import org.apache.reef.tang.types.Node;

import java.util.Arrays;
import java.util.List;

public class ProtocolBufferInjectionPlan {

  <T> InjectionPlanProto.InjectionPlan newConstructor(final String fullName,
                                                      List<InjectionPlanProto.InjectionPlan> plans) {
    return InjectionPlanProto.InjectionPlan
        .newBuilder()
        .setName(fullName)
        .setConstructor(
            InjectionPlanProto.Constructor.newBuilder().addAllArgs(plans)
                .build()).build();
  }

  <T> InjectionPlanProto.InjectionPlan newSubplan(final String fullName,
                                                  int selectedPlan, List<InjectionPlanProto.InjectionPlan> plans) {
    return InjectionPlanProto.InjectionPlan
        .newBuilder()
        .setName(fullName)
        .setSubplan(
            InjectionPlanProto.Subplan.newBuilder()
                .setSelectedPlan(selectedPlan).addAllPlans(plans).build())
        .build();
  }

  <T> InjectionPlanProto.InjectionPlan newInstance(final String fullName,
                                                   final String value) {
    return InjectionPlanProto.InjectionPlan.newBuilder().setName(fullName)
        .setInstance(InjectionPlanProto.Instance.newBuilder().setValue(value))
        .build();
  }

  public <T> InjectionPlanProto.InjectionPlan serialize(InjectionPlan<T> ip) {
    if (ip instanceof Constructor) {
      Constructor<T> cons = (Constructor<T>) ip;
      InjectionPlan<?>[] args = cons.getArgs();
      InjectionPlanProto.InjectionPlan[] protoArgs = new InjectionPlanProto.InjectionPlan[args.length];
      for (int i = 0; i < args.length; i++) {
        protoArgs[i] = serialize(args[i]);
      }
      return newConstructor(ip.getNode().getFullName(),
          Arrays.asList(protoArgs));
    } else if (ip instanceof Subplan) {
      Subplan<T> sp = (Subplan<T>) ip;
      InjectionPlan<?>[] args = sp.getPlans();
      InjectionPlanProto.InjectionPlan[] subPlans = new InjectionPlanProto.InjectionPlan[args.length];
      for (int i = 0; i < args.length; i++) {
        subPlans[i] = serialize(args[i]);
      }
      return newSubplan(ip.getNode().getFullName(), sp.getSelectedIndex(),
          Arrays.asList(subPlans));
    } else if (ip instanceof JavaInstance) {
      JavaInstance<T> ji = (JavaInstance<T>) ip;
      return newInstance(ip.getNode().getFullName(), ji.getInstanceAsString());
    } else {
      throw new IllegalStateException(
          "Encountered unknown type of InjectionPlan: " + ip);
    }
  }

  private Object parse(String type, String value) {
    // XXX this is a placeholder for now.  We need a parser API that will
    // either produce a live java object or (partially) validate stuff to
    // see if it looks like the target language will be able to handle this
    // type + value.
    return value;
  }

  @SuppressWarnings("unchecked")
  public <T> InjectionPlan<T> deserialize(ClassHierarchy ch,
                                          InjectionPlanProto.InjectionPlan ip) throws NameResolutionException,
      BindException {
    final String fullName = ip.getName();
    if (ip.hasConstructor()) {
      final InjectionPlanProto.Constructor cons = ip.getConstructor();

      final ClassNode<T> cn = (ClassNode<T>) ch.getNode(fullName);

      final InjectionPlanProto.InjectionPlan protoBufArgs[] = cons
          .getArgsList().toArray(new InjectionPlanProto.InjectionPlan[0]);
      final ClassNode<?>[] cnArgs = new ClassNode[protoBufArgs.length];

      for (int i = 0; i < protoBufArgs.length; i++) {
        cnArgs[i] = (ClassNode<?>) ch.getNode(protoBufArgs[i].getName());
      }

      final InjectionPlan<?> ipArgs[] = new InjectionPlan[protoBufArgs.length];

      for (int i = 0; i < protoBufArgs.length; i++) {
        ipArgs[i] = (InjectionPlan<?>) deserialize(ch, protoBufArgs[i]);
      }

      final ConstructorDef<T> constructor = cn.getConstructorDef(cnArgs);
      return new Constructor<T>(cn, constructor, ipArgs);
    } else if (ip.hasInstance()) {
      InjectionPlanProto.Instance ins = ip.getInstance();
      T instance = (T) parse(ip.getName(), ins.getValue());
      return new JavaInstance<T>(ch.getNode(ip.getName()), instance);
    } else if (ip.hasSubplan()) {
      final InjectionPlanProto.Subplan subplan = ip.getSubplan();
      final InjectionPlanProto.InjectionPlan protoBufPlans[] = subplan
          .getPlansList().toArray(new InjectionPlanProto.InjectionPlan[0]);

      final InjectionPlan<T> subPlans[] = new InjectionPlan[protoBufPlans.length];
      for (int i = 0; i < protoBufPlans.length; i++) {
        subPlans[i] = (InjectionPlan<T>) deserialize(ch, protoBufPlans[i]);
      }
      Node n = ch.getNode(fullName);
      return new Subplan<T>(n, subPlans);
    } else {
      throw new IllegalStateException(
          "Encountered unknown type of injection plan: " + ip);
    }
  }
}

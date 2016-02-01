/*
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
package org.apache.reef.tang.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A TANG Unit consists of an outer class and some non-static inner classes.
 * TANG injectors automatically treat all the classes in a unit as singletons.
 * <p>
 * In order to inject the singleton instance of each inner class, TANG first
 * instantiates the outer class and then uses the resulting instance to
 * instantiate each inner class.
 * <p>
 * Classes annotated in this way must have at least one non-static inner class
 * and no static inner classes. The inner classes must not declare any
 * constructors.
 * <p>
 * Furthermore, classes annotated with Unit may not have injectable (or Unit)
 * subclasses.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Unit {

}

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
/**
 * Private implementation classes that configure and inject code written
 * in Java.  Tang supports cross-language object injections.  The classes
 * in com.microsoft.tang.implementation are agnostic to the application
 * language.  In contrast, the classes in this package provide convenience
 * APIs that move type-checking to Java's generic type system, and are also
 * responsible for actually injecting objects (since, by definition, Java
 * JVMs inject Java objects).  
 */
package com.microsoft.tang.implementation.java;


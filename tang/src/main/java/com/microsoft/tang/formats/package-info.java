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
 * Tang format classes encode and decode information that Tang gathers at
 * runtime.  Such information comes in three forms: ClassHierarchy data
 * that is derived directly from compiled application code, Configuration
 * data that has been typechecked, and is derived from various user inputs,
 * such as configuration files and command line arguments, and finally,
 * InjectionPlans which encode the constructor invocations that Tang would
 * make when instantiating a particular class.
 */
package com.microsoft.tang.formats;


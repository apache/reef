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
 * Representations of the Client (i.e. the job submitter) to a Driver.
 * Note that as of REEF 0.3, the client is optional and may not be available to a driver.
 */
@DriverSide package com.microsoft.reef.driver.client;

import com.microsoft.reef.annotations.audience.DriverSide;
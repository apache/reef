/*
 * Copyright 2012 Microsoft.
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
package com.microsoft.tang.exceptions;

/**
 *
 * @author Markus Weimer <mweimer@microsoft.com>
 */
public class InjectionException extends Exception {

    public InjectionException() {
    }

    public InjectionException(String string) {
        super(string);
    }

    public InjectionException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public InjectionException(Throwable thrwbl) {
        super(thrwbl);
    }

    public InjectionException(String string, Throwable thrwbl, boolean bln, boolean bln1) {
        super(string, thrwbl, bln, bln1);
    }
}

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

package com.microsoft.tang.implementation.java;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.formats.avro.AvroConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.inject.Inject;

/**
 * TestConfigurationBuilder
 */
public class TestConfigurationBuilder {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void nullStringVaueTest() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("The value null set to the named parameter is illegal: class com.microsoft.tang.implementation.java.TestConfigurationBuilder$NamedParamterNoDefault$NamedString");

        Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(NamedParamterNoDefault.NamedString.class, (String) null)
                .build();
    }

    static class NamedParamterNoDefault {
        final private String str;

        @NamedParameter()
        class NamedString implements Name<String> {
        }

        @Inject
        NamedParamterNoDefault(@Parameter(NamedString.class) String str) {
            this.str = str;
        }
    }
}

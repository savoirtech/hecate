/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.pojo.convert.enumeration;

import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.cql.Gender;
import com.savoirtech.hecate.test.AbstractTestCase;
import org.junit.Test;

public class EnumConverterProviderTest extends AbstractTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testCreateConverter() {
        EnumConverterProvider provider = new EnumConverterProvider();
        assertEquals(EnumConverter.class, provider.converterType());
        Converter converter = provider.createConverter(Gender.class);
        assertTrue(converter instanceof EnumConverter);
        EnumConverter enumConverter = (EnumConverter) converter;
        assertEquals(Gender.class, enumConverter.getEnumType());
    }
}
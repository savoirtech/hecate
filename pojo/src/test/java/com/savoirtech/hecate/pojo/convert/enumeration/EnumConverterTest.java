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

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.pojo.util.Gender;
import com.savoirtech.hecate.test.AbstractTestCase;
import org.junit.Test;

public class EnumConverterTest extends AbstractTestCase {

    @Test
    public void testDataType() {
        EnumConverter converter = new EnumConverter(Gender.class);
        assertEquals(DataType.varchar(), converter.getDataType());
    }

    @Test
    public void testToColumnValue() {
        EnumConverter converter = new EnumConverter(Gender.class);
        assertNull(converter.toColumnValue(null));
        assertEquals("MALE", converter.toColumnValue(Gender.MALE));
    }

    @Test
    public void testToFacetValue() {
        EnumConverter converter = new EnumConverter(Gender.class);
        assertNull(converter.toFacetValue(null));
        assertEquals(Gender.FEMALE, converter.toFacetValue("FEMALE"));
    }

}
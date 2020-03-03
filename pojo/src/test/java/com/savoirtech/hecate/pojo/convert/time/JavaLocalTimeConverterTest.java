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

package com.savoirtech.hecate.pojo.convert.time;

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalTime;

public class JavaLocalTimeConverterTest extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testGetDataType() {
        assertEquals(DataTypes.TEXT, new JavaLocalTimeConverter().getDataType());
    }

    @Test
    public void testGetValueType() {
        assertEquals(LocalTime.class, new JavaLocalTimeConverter().getValueType());
    }

    @Test
    public void testRoundTrip() {
        LocalTime now = LocalTime.now();

        JavaLocalTimeConverter converter = new JavaLocalTimeConverter();
        Object columnValue = converter.toColumnValue(now);
        assertEquals(now, converter.toFacetValue(columnValue));
    }

    @Test
    public void testToColumnValue() {
        LocalTime now = LocalTime.now();

        JavaLocalTimeConverter converter = new JavaLocalTimeConverter();
        assertNull(converter.toColumnValue(null));
        assertEquals(now.toString(), converter.toColumnValue(now));
    }

    @Test
    public void testToFacetValue() {
        LocalTime now = LocalTime.now();

        JavaLocalTimeConverter converter = new JavaLocalTimeConverter();
        assertNull(converter.toFacetValue(null));
        assertEquals(now, converter.toFacetValue(now.toString()));
    }
}
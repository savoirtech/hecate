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

import com.datastax.driver.core.DataType;
import org.junit.Assert;
import org.junit.Test;

import java.time.OffsetTime;

public class JavaOffsetTimeConverterTest extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testGetDataType() {
        assertEquals(DataType.varchar(), new JavaOffsetTimeConverter().getDataType());
    }

    @Test
    public void testGetValueType() {
        assertEquals(OffsetTime.class, new JavaOffsetTimeConverter().getValueType());
    }

    @Test
    public void testRoundTrip() {
        OffsetTime now = OffsetTime.now();

        JavaOffsetTimeConverter converter = new JavaOffsetTimeConverter();
        Object columnValue = converter.toColumnValue(now);
        assertEquals(now, converter.toFacetValue(columnValue));
    }

    @Test
    public void testToColumnValue() {
        OffsetTime now = OffsetTime.now();

        JavaOffsetTimeConverter converter = new JavaOffsetTimeConverter();
        assertNull(converter.toColumnValue(null));
        assertEquals(now.toString(), converter.toColumnValue(now));
    }

    @Test
    public void testToFacetValue() {
        OffsetTime now = OffsetTime.now();

        JavaOffsetTimeConverter converter = new JavaOffsetTimeConverter();
        assertNull(converter.toFacetValue(null));
        assertEquals(now, converter.toFacetValue(now.toString()));
    }
}
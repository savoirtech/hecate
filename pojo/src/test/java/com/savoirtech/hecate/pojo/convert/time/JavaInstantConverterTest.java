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

import java.time.Instant;

public class JavaInstantConverterTest extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testToColumnValue() {
        Instant now = Instant.now();

        JavaInstantConverter converter = new JavaInstantConverter();
        assertNull(converter.toColumnValue(null));
        assertEquals(now.toString(), converter.toColumnValue(now));
    }

    @Test
    public void testToFacetValue() {
        Instant now = Instant.now();

        JavaInstantConverter converter = new JavaInstantConverter();
        assertNull(converter.toFacetValue(null));
        assertEquals(now, converter.toFacetValue(now.toString()));
    }

    @Test
    public void testGetValueType() {
        assertEquals(Instant.class, new JavaInstantConverter().getValueType());
    }

    @Test
    public void testGetDataType() {
        assertEquals(DataType.varchar(), new JavaInstantConverter().getDataType());
    }

    @Test
    public void testRoundTrip() {
        Instant now = Instant.now();

        JavaInstantConverter converter = new JavaInstantConverter();
        Object columnValue = converter.toColumnValue(now);
        assertEquals(now, converter.toFacetValue(columnValue));
    }
}
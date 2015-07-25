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

import java.time.Duration;

public class JavaDurationConverterTest extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testToColumnValue() {
        Duration oneMin = Duration.ofMinutes(1);

        JavaDurationConverter converter = new JavaDurationConverter();
        assertNull(converter.toColumnValue(null));
        assertEquals(oneMin.toString(), converter.toColumnValue(oneMin));
    }

    @Test
    public void testToFacetValue() {
        Duration oneMin = Duration.ofMinutes(1);

        JavaDurationConverter converter = new JavaDurationConverter();
        assertNull(converter.toFacetValue(null));
        assertEquals(oneMin, converter.toFacetValue(oneMin.toString()));
    }

    @Test
    public void testGetValueType() {
        assertEquals(Duration.class, new JavaDurationConverter().getValueType());
    }

    @Test
    public void testGetDataType() {
        assertEquals(DataType.varchar(), new JavaDurationConverter().getDataType());
    }

    @Test
    public void testRoundTrip() {
        Duration oneMin = Duration.ofMinutes(1);

        JavaDurationConverter converter = new JavaDurationConverter();
        Object columnValue = converter.toColumnValue(oneMin);
        assertEquals(oneMin, converter.toFacetValue(columnValue));
    }
}
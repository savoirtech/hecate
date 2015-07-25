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

import java.time.Period;

public class JavaPeriodConverterTest extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testToColumnValue() {
        Period oneMin = Period.ofYears(1);

        JavaPeriodConverter converter = new JavaPeriodConverter();
        assertNull(converter.toColumnValue(null));
        assertEquals(oneMin.toString(), converter.toColumnValue(oneMin));
    }

    @Test
    public void testToFacetValue() {
        Period oneMin = Period.ofYears(1);

        JavaPeriodConverter converter = new JavaPeriodConverter();
        assertNull(converter.toFacetValue(null));
        assertEquals(oneMin, converter.toFacetValue(oneMin.toString()));
    }

    @Test
    public void testGetValueType() {
        assertEquals(Period.class, new JavaPeriodConverter().getValueType());
    }

    @Test
    public void testGetDataType() {
        assertEquals(DataType.varchar(), new JavaPeriodConverter().getDataType());
    }

    @Test
    public void testRoundTrip() {
        Period oneMin = Period.ofYears(1);

        JavaPeriodConverter converter = new JavaPeriodConverter();
        Object columnValue = converter.toColumnValue(oneMin);
        assertEquals(oneMin, converter.toFacetValue(columnValue));
    }
}
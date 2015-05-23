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

package com.savoirtech.hecate.pojo.convert.binary;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.test.AbstractTestCase;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ByteArrayConverterTest extends AbstractTestCase {

    @Test
    public void testToFacetValue() throws Exception {
        ByteArrayConverter converter = new ByteArrayConverter();
        assertNull(converter.toFacetValue(null));
        byte[] bytes = "Hello".getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertArrayEquals(bytes, (byte[]) converter.toFacetValue(buffer));
    }

    @Test
    public void testGetDataType() throws Exception {
        ByteArrayConverter converter = new ByteArrayConverter();
        assertEquals(DataType.blob(),converter.getDataType());
    }

    @Test
    public void testToColumnValue() throws Exception {
        ByteArrayConverter converter = new ByteArrayConverter();
        assertNull(converter.toColumnValue(null));
        byte[] bytes = "Hello".getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(buffer, converter.toColumnValue(bytes));
    }
}
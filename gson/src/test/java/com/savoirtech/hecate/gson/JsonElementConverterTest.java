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

package com.savoirtech.hecate.gson;

import com.datastax.driver.core.DataType;
import com.google.gson.*;
import com.savoirtech.hecate.pojo.convert.def.DefaultConverterRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JsonElementConverterTest extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private JsonArray array;
    private JsonObject object;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void setUpData() {
        array = new JsonArray();
        array.add(new JsonPrimitive(1));
        array.add(new JsonPrimitive(2));
        array.add(new JsonPrimitive(3));

        object = new JsonObject();
        object.addProperty("a", "foo");
        object.addProperty("b", 1);
        object.addProperty("c", false);
    }

    @Test
    public void testGetDataType() {
        assertEquals(DataType.varchar(), new JsonElementConverter().getDataType());
    }

    @Test
    public void testGetValueType() {
        assertEquals(JsonElement.class, new JsonElementConverter().getValueType());
    }

    @Test
    public void testRegistration() {
        DefaultConverterRegistry reg = new DefaultConverterRegistry();
        assertNotNull(reg.getConverter(JsonElement.class));
    }

    @Test
    public void testToColumnValue() {
        JsonElementConverter converter = new JsonElementConverter();
        assertNull(converter.toColumnValue(null));
        assertEquals("null", converter.toColumnValue(JsonNull.INSTANCE));
        assertEquals("\"hello\"", converter.toColumnValue(new JsonPrimitive("hello")));
        assertEquals("1", converter.toColumnValue(new JsonPrimitive(1)));
        assertEquals("false", converter.toColumnValue(new JsonPrimitive(false)));

        assertEquals("[1,2,3]", converter.toColumnValue(array));

        assertEquals(object.toString(), converter.toColumnValue(object));
    }

    @Test
    public void testToFacetValue() {
        JsonElementConverter converter = new JsonElementConverter();
        assertNull(converter.toFacetValue(null));
        assertEquals(JsonNull.INSTANCE, converter.toFacetValue("null"));
        assertEquals(new JsonPrimitive("foo"), converter.toFacetValue("\"foo\""));
        assertEquals(new JsonPrimitive(1), converter.toFacetValue("1"));
        assertEquals(new JsonPrimitive(false), converter.toFacetValue("false"));
        assertEquals(array, converter.toFacetValue(array.toString()));
        assertEquals(object, converter.toFacetValue(object.toString()));
    }
}
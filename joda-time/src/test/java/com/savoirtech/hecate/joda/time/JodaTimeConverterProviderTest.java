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

package com.savoirtech.hecate.joda.time;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.def.DefaultConverterRegistry;
import org.junit.Assert;
import org.junit.Test;

public abstract class JodaTimeConverterProviderTest extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final String GARBAGE = "GARBAGE STRING";
    private final JodaTimeConverterProvider provider;
    private final Class<?> expectedBaseType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public JodaTimeConverterProviderTest(JodaTimeConverterProvider provider, Class<?> expectedBaseType) {
        this.provider = provider;
        this.expectedBaseType = expectedBaseType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected <T> void assertSupportsType(Class<T> jodaType, T instance) {
        Converter converter = new DefaultConverterRegistry().getConverter(jodaType);
        assertNotNull(converter);
        assertEquals(DataType.varchar(), converter.getDataType());
        assertEquals(jodaType, converter.getValueType());
        assertNull(converter.toColumnValue(null));
        assertNull(converter.toFacetValue(null));
        assertEquals(instance.toString(), converter.toColumnValue(instance));
        assertEquals(instance, converter.toFacetValue(instance.toString()));
        try {
            converter.toFacetValue(GARBAGE);
            fail("Unsupported string should throw HecateException!");
        }
        catch(HecateException e) {
            assertEquals(String.format("Unable to parse '%s' value into %s.", GARBAGE, jodaType.getCanonicalName()),e.getMessage());
        }
    }
    
    @Test
    public void testExpectedBaseType() {
        assertEquals(expectedBaseType, provider.getValueType());
    }

    @Test
    public void testGetConverterType() {
        assertEquals(JodaTimeConverter.class, provider.converterType());
    }

    @Test(expected = HecateException.class)
    public void testNonConcreteType() {
        provider.createConverter(expectedBaseType);
    }

    @Test(expected = HecateException.class)
    public void testWithUnsupportedType() {
        provider.createConverter(String.class);
    }
}

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

package com.savoirtech.hecate.pojo.convert.def;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.time.*;
import java.util.UUID;

import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.cql.Gender;
import com.savoirtech.hecate.pojo.type.GenericType;
import org.junit.Test;

public class DefaultConverterRegistryTest {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ConverterRegistry registry = new DefaultConverterRegistry();

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testDefaultRegistry() throws Exception {
        assertNotNull(registry.getConverter(Boolean.class));
        assertNotNull(registry.getConverter(BigDecimal.class));
        assertNotNull(registry.getConverter(BigInteger.class));
        assertNotNull(registry.getConverter(Double.class));
        assertNotNull(registry.getConverter(Float.class));
        assertNotNull(registry.getConverter(InetAddress.class));
        assertNotNull(registry.getConverter(Integer.class));
        assertNotNull(registry.getConverter(String.class));
        assertNotNull(registry.getConverter(UUID.class));
        assertNotNull(registry.getConverter(Enum.class));
        assertNotNull(registry.getConverter(ByteBuffer.class));
        assertNotNull(registry.getConverter(byte[].class));
        assertNotNull(registry.getConverter(Duration.class));
        assertNotNull(registry.getConverter(Instant.class));
        assertNotNull(registry.getConverter(LocalDate.class));
        assertNotNull(registry.getConverter(LocalDateTime.class));
        assertNotNull(registry.getConverter(LocalTime.class));
        assertNotNull(registry.getConverter(OffsetDateTime.class));
        assertNotNull(registry.getConverter(OffsetTime.class));
        assertNotNull(registry.getConverter(Period.class));
        assertNotNull(registry.getConverter(Gender.class));
    }

    @Test
    public void testGetConverter() {
        assertNull(registry.getConverter((Class<?>) null));
        assertNull(registry.getConverter(Connection.class));
        assertNull(registry.getConverter(Connection.class));
    }

    @Test
    public void testGetConverterByGenericType() throws Exception {
        GenericType genericType1 = new GenericType(Fields.class, Fields.class.getField("field").getGenericType());
        GenericType genericType2 = new GenericType(Fields.class, Fields.class.getField("connection").getGenericType());
        assertNull(registry.getConverter((GenericType) null));
        assertNotNull(registry.getConverter(genericType1));
        assertNull(registry.getConverter(genericType2));
    }

    @Test
    public void testGetConverterForSubclass() {
        assertNotNull(registry.getConverter(Gender.class));
    }

    @Test
    public void testGetRequiredConverterByGenericType() throws Exception {
        GenericType type = new GenericType(Fields.class, Fields.class.getField("field").getGenericType());
        registry.getRequiredConverter(type, "No converter found!");
    }

    @Test(expected = HecateException.class)
    public void testGetRequiredConverterByGenericTypeWhenNotFound() throws Exception {
        GenericType type = new GenericType(Fields.class, Fields.class.getField("connection").getGenericType());
        registry.getRequiredConverter(type, "No converter found");
    }

    @Test
    public void testGetRequiredConverterWhenFound() {
        assertNotNull(registry.getRequiredConverter(String.class, "No converter found"));
    }

    @Test(expected = HecateException.class)
    public void testGetRequiredConverterWhenNotFound() {
        registry.getRequiredConverter(Connection.class, "No converter found");
    }

    @Test(expected = HecateException.class)
    public void testGetRequiredConverterWithNullClass() {
        registry.getRequiredConverter((Class<?>) null, "No converter found");
    }

    @Test(expected = HecateException.class)
    public void testGetRequiredConverterWithNullGenericType() {
        registry.getRequiredConverter((GenericType) null, "No converter found");
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class Fields {
        public String field;
        public Connection connection;
    }
}
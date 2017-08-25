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

package com.savoirtech.hecate.pojo.convert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;

public final class NativeConverter implements Converter {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final Converter BOOLEAN = new NativeConverter(Boolean.class, DataType.cboolean());
    public static final Converter BOOLEAN_TYPE = new NativeConverter(Boolean.TYPE, DataType.cboolean(), Boolean.FALSE);

    public static final Converter DATE = new NativeConverter(LocalDate.class, DataType.date());

    public static final Converter TIMESTAMP = new NativeConverter(Date.class, DataType.timestamp());

    public static final Converter DOUBLE = new NativeConverter(Double.class, DataType.cdouble());
    public static final Converter DOUBLE_TYPE = new NativeConverter(Double.TYPE, DataType.cdouble(), 0.0);

    public static final Converter FLOAT = new NativeConverter(Float.class, DataType.cfloat());
    public static final Converter FLOAT_TYPE = new NativeConverter(Float.TYPE, DataType.cfloat(), 0.0f);

    public static final Converter INTEGER = new NativeConverter(Integer.class, DataType.cint());
    public static final Converter INTEGER_TYPE = new NativeConverter(Integer.TYPE, DataType.cint(), 0);

    public static final Converter LONG = new NativeConverter(Long.class, DataType.bigint());
    public static final Converter LONG_TYPE = new NativeConverter(Long.TYPE, DataType.bigint(), 0L);

    public static final Converter BYTE = new NativeConverter(Byte.class, DataType.tinyint());
    public static final Converter BYTE_TYPE = new NativeConverter(Byte.TYPE, DataType.tinyint());

    public static final Converter SHORT = new NativeConverter(Short.class, DataType.smallint());
    public static final Converter SHORT_TYPE = new NativeConverter(Short.TYPE, DataType.smallint());

    public static final Converter UUID = new NativeConverter(java.util.UUID.class, DataType.uuid());
    public static final Converter STRING = new NativeConverter(String.class, DataType.varchar());
    public static final Converter INET = new NativeConverter(InetAddress.class, DataType.inet());
    public static final Converter BIG_DECIMAL = new NativeConverter(BigDecimal.class, DataType.decimal());
    public static final Converter BIG_INTEGER = new NativeConverter(BigInteger.class, DataType.varint());
    public static final Converter BLOB = new NativeConverter(ByteBuffer.class, DataType.blob());

    private final Class<?> valueType;
    private final DataType dataType;
    private final Object defaultFacetValue;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    private NativeConverter(Class<?> valueType, DataType dataType) {
        this(valueType, dataType, null);
    }

    public NativeConverter(Class<?> valueType, DataType dataType, Object defaultFacetValue) {
        this.valueType = valueType;
        this.dataType = dataType;
        this.defaultFacetValue = defaultFacetValue;
    }

//----------------------------------------------------------------------------------------------------------------------
// Converter Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public Class<?> getValueType() {
        return valueType;
    }

    @Override
    public Object toColumnValue(Object value) {
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object toFacetValue(Object value) {
        return value == null ? defaultFacetValue : value;
    }
}

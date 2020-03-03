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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;

public final class NativeConverter implements Converter {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final Converter BOOLEAN = new NativeConverter(Boolean.class, DataTypes.BOOLEAN);
    public static final Converter BOOLEAN_TYPE = new NativeConverter(Boolean.TYPE, DataTypes.BOOLEAN, Boolean.FALSE);

    public static final Converter DATE = new NativeConverter(LocalDate.class, DataTypes.DATE);

    public static final Converter TIMESTAMP = new NativeConverter(Instant.class, DataTypes.TIMESTAMP);

    public static final Converter DOUBLE = new NativeConverter(Double.class, DataTypes.DOUBLE);
    public static final Converter DOUBLE_TYPE = new NativeConverter(Double.TYPE, DataTypes.DOUBLE, 0.0);

    public static final Converter FLOAT = new NativeConverter(Float.class, DataTypes.FLOAT);
    public static final Converter FLOAT_TYPE = new NativeConverter(Float.TYPE, DataTypes.FLOAT, 0.0f);

    public static final Converter INTEGER = new NativeConverter(Integer.class, DataTypes.INT);
    public static final Converter INTEGER_TYPE = new NativeConverter(Integer.TYPE, DataTypes.INT, 0);

    public static final Converter LONG = new NativeConverter(Long.class, DataTypes.BIGINT);
    public static final Converter LONG_TYPE = new NativeConverter(Long.TYPE, DataTypes.BIGINT, 0L);

    public static final Converter BYTE = new NativeConverter(Byte.class, DataTypes.TINYINT);
    public static final Converter BYTE_TYPE = new NativeConverter(Byte.TYPE, DataTypes.TINYINT);

    public static final Converter SHORT = new NativeConverter(Short.class, DataTypes.SMALLINT);
    public static final Converter SHORT_TYPE = new NativeConverter(Short.TYPE, DataTypes.SMALLINT);

    public static final Converter UUID = new NativeConverter(java.util.UUID.class, DataTypes.UUID);
    public static final Converter STRING = new NativeConverter(String.class, DataTypes.TEXT);
    public static final Converter INET = new NativeConverter(InetAddress.class, DataTypes.INET);
    public static final Converter BIG_DECIMAL = new NativeConverter(BigDecimal.class, DataTypes.DECIMAL);
    public static final Converter BIG_INTEGER = new NativeConverter(BigInteger.class, DataTypes.VARINT);
    public static final Converter BLOB = new NativeConverter(ByteBuffer.class, DataTypes.BLOB);

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

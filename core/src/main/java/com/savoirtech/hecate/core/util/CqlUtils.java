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

package com.savoirtech.hecate.core.util;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.LinkedList;
import java.util.List;
import com.savoirtech.hecate.core.exception.HecateException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlUtils {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final Logger CQL_LOGGER = LoggerFactory.getLogger(CqlUtils.class);

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static BoundStatement bind(PreparedStatement statement, Object[] params) {
        if(CQL_LOGGER.isDebugEnabled()) {
            CQL_LOGGER.debug("{} parameters ({})", statement.getQuery(), StringUtils.join(params, ","));
        }
        return statement.bind(params);
    }

    public static Object getValue(Row row, int columnIndex, DataType dataType) {
        if(row.isNull(columnIndex)) {
            return null;
        }

        switch (dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.ASCII:
                return row.getString(columnIndex);
            case ProtocolConstants.DataType.BIGINT:
                return row.getLong(columnIndex);
            case ProtocolConstants.DataType.BLOB:
                return row.getByteBuffer(columnIndex);
            case ProtocolConstants.DataType.BOOLEAN:
                return row.getBoolean(columnIndex);
            case ProtocolConstants.DataType.COUNTER:
                return row.getLong(columnIndex);
            case ProtocolConstants.DataType.DECIMAL:
                return row.getBigDecimal(columnIndex);
            case ProtocolConstants.DataType.DOUBLE:
                return row.getDouble(columnIndex);
            case ProtocolConstants.DataType.FLOAT:
                return row.getFloat(columnIndex);
            case ProtocolConstants.DataType.INT:
                return row.getInt(columnIndex);
            case ProtocolConstants.DataType.TIMESTAMP:
                return row.getInstant(columnIndex);
            case ProtocolConstants.DataType.UUID:
                return row.getUuid(columnIndex);
            case ProtocolConstants.DataType.VARINT:
                return row.getBigInteger(columnIndex);
            case ProtocolConstants.DataType.TIMEUUID:
                return row.getUuid(columnIndex);
            case ProtocolConstants.DataType.INET:
                return row.getInetAddress(columnIndex);
            case ProtocolConstants.DataType.DATE:
                return row.getLocalDate(columnIndex);
            case ProtocolConstants.DataType.TIME:
                return row.getLocalTime(columnIndex);
            case ProtocolConstants.DataType.SMALLINT:
                return row.getShort(columnIndex);
            case ProtocolConstants.DataType.TINYINT:
                return row.getByte(columnIndex);
            case ProtocolConstants.DataType.DURATION:
                return row.getCqlDuration(columnIndex);
            case ProtocolConstants.DataType.LIST:
                return row.getList(columnIndex, typeArgument(row, ((ListType) dataType).getElementType()));
            case ProtocolConstants.DataType.SET:
                return row.getSet(columnIndex, typeArgument(row, ((SetType) dataType).getElementType()));
            case ProtocolConstants.DataType.MAP:
                return row.getMap(columnIndex, typeArgument(row, ((MapType) dataType).getKeyType()), typeArgument(row, ((MapType) dataType).getValueType()));
            case ProtocolConstants.DataType.TUPLE:
                return row.getTupleValue(columnIndex);
            case ProtocolConstants.DataType.VARCHAR:
                return row.getString(columnIndex);
            default:
                throw new HecateException(String.format("Unsupported data type %s.", dataType.asCql(true, true)));
        }
    }

    public static List<Object> toList(Row row) {
        List<Object> results = new LinkedList<>();
        int index = 0;
        for (ColumnDefinition def : row.getColumnDefinitions()) {
            results.add(getValue(row, index, def.getType()));
            index++;
        }
        return results;
    }

    private static Class<?> typeArgument(Row row, DataType dataType) {
        final TypeCodec<Object> codec = row.codecRegistry().codecFor(dataType);
        return codec.getJavaType().getRawType();
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    private CqlUtils() {
        // Prevent instantiation!
    }
}

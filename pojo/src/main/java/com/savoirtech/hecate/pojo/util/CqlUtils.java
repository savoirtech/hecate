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

package com.savoirtech.hecate.pojo.util;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.core.exception.HecateException;

import java.util.LinkedList;
import java.util.List;

public class CqlUtils {
//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static Object getValue(Row row, int columnIndex, DataType dataType) {
        switch (dataType.getName()) {
            case ASCII:
            case VARCHAR:
            case TEXT:
                return row.getString(columnIndex);
            case BIGINT:
                return row.getLong(columnIndex);
            case BOOLEAN:
                return row.getBool(columnIndex);
            case DECIMAL:
                return row.getDecimal(columnIndex);
            case DOUBLE:
                return row.getDouble(columnIndex);
            case FLOAT:
                return row.getFloat(columnIndex);
            case INT:
                return row.getInt(columnIndex);
            case TIMESTAMP:
                return row.getDate(columnIndex);
            case UUID:
            case TIMEUUID:
                return row.getUUID(columnIndex);
            case LIST:
                return row.getList(columnIndex, typeArgument(dataType, 0));
            case SET:
                return row.getSet(columnIndex, typeArgument(dataType, 0));
            case MAP:
                return row.getMap(columnIndex, typeArgument(dataType, 0), typeArgument(dataType, 1));
            case BLOB:
                return row.getBytes(columnIndex);
            case INET:
                return row.getInet(columnIndex);
            case VARINT:
                return row.getVarint(columnIndex);
            case TUPLE:
                return row.getTupleValue(columnIndex);
            default:
                throw new HecateException(String.format("Unsupported data type %s.", dataType.getName()));
        }
    }

    public static List<Object> toList(Row row) {
        List<Object> results = new LinkedList<>();
        int index = 0;
        for (ColumnDefinitions.Definition def : row.getColumnDefinitions()) {
            results.add(getValue(row, index, def.getType()));
            index++;
        }
        return results;
    }

    private static Class<?> typeArgument(DataType dataType, int index) {
        return dataType.getTypeArguments().get(index).asJavaClass();
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    private CqlUtils() {
        // Prevent instantiation!
    }
}

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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.datastax.driver.core.*;
import com.savoirtech.hecate.core.exception.HecateException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlUtils {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final Logger CQL_LOGGER = LoggerFactory.getLogger("com.savoirtech.hecate.cql");

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static BoundStatement bind(PreparedStatement statement, Object[] params) {
        if(CQL_LOGGER.isDebugEnabled()) {
            CQL_LOGGER.debug("{} parameters ({})", statement.getQueryString(), StringUtils.join(params, ","));
        }
        return statement.bind(params);
    }

    public static Object getValue(GettableByIndexData gettable, int columnIndex, DataType dataType) {
        if(gettable.isNull(columnIndex)) {
            return null;
        }
        switch (dataType.getName()) {
            case ASCII:
            case VARCHAR:
            case TEXT:
                return gettable.getString(columnIndex);
            case BIGINT:
                return gettable.getLong(columnIndex);
            case BOOLEAN:
                return gettable.getBool(columnIndex);
            case DECIMAL:
                return gettable.getDecimal(columnIndex);
            case DOUBLE:
                return gettable.getDouble(columnIndex);
            case FLOAT:
                return gettable.getFloat(columnIndex);
            case INT:
                return gettable.getInt(columnIndex);
            case TIMESTAMP:
                return gettable.getDate(columnIndex);
            case UUID:
            case TIMEUUID:
                return gettable.getUUID(columnIndex);
            case LIST:
                return gettable.getList(columnIndex, typeArgument(dataType, 0));
            case SET:
                return gettable.getSet(columnIndex, typeArgument(dataType, 0));
            case MAP:
                return gettable.getMap(columnIndex, typeArgument(dataType, 0), typeArgument(dataType, 1));
            case BLOB:
                return gettable.getBytes(columnIndex);
            case INET:
                return gettable.getInet(columnIndex);
            case VARINT:
                return gettable.getVarint(columnIndex);
            case TUPLE:
                return gettable.getTupleValue(columnIndex);
            default:
                throw new HecateException(String.format("Unsupported data type %s.", dataType.getName()));
        }
    }

    public static List<Object> toList(TupleValue tuple) {
        List<DataType> componentTypes = tuple.getType().getComponentTypes();
        List<Object> results = new ArrayList<>(componentTypes.size());
        int index = 0;
        for (DataType dataType : componentTypes) {
            results.add(getValue(tuple, index++, dataType));
        }
        return results;
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

/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.util;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.annotations.Column;
import com.savoirtech.hecate.cql3.annotations.Id;
import com.savoirtech.hecate.cql3.annotations.Index;
import com.savoirtech.hecate.cql3.annotations.Table;
import com.savoirtech.hecate.cql3.exception.HecateException;
import com.savoirtech.hecate.cql3.value.Facet;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class CassandraUtils {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final double UNSPECIFIED_CHANCE = -1.0;
    public static final long UNSPECIFIED_TIME = -1;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static String columnName(Facet facet) {
        Column annot = facet.getAnnotation(Column.class);
        if (annot != null && StringUtils.isNotEmpty(annot.name())) {
            return annot.name();
        }
        return facet.getName();
    }

    private static GenericType getElementType(Facet facet) {
        final GenericType facetType = facet.getType();
        final Class<?> facetRawType = facetType.getRawType();
        if (List.class.equals(facetRawType)) {
            return facetType.getListElementType();
        }
        if (Set.class.equals(facetRawType)) {
            return facetType.getSetElementType();
        }
        if (Map.class.equals(facetRawType)) {
            return facetType.getMapValueType();
        }
        if (facetRawType.isArray()) {
            return facetType.getArrayElementType();
        }
        return facetType;
    }

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
                return row.getUUID(columnIndex);
            case LIST:
                return row.getList(columnIndex, typeArgument(dataType, 0));
            case SET:
                return row.getSet(columnIndex, typeArgument(dataType, 0));
            case MAP:
                return row.getMap(columnIndex, typeArgument(dataType, 0), typeArgument(dataType, 1));
            case BLOB:
                return row.getBytes(columnIndex);
            case COUNTER:
                return row.getLong(columnIndex);
            case INET:
                return row.getInet(columnIndex);
            case VARINT:
                return row.getVarint(columnIndex);
            default:
                throw new HecateException(String.format("Unsupported data type %s.", dataType.getName()));
        }
    }

    public static boolean isIdentifier(Facet facet) {
        return facet.getAnnotation(Id.class) != null;
    }

    public static String tableName(Class<?> pojoType) {
        Table annot = pojoType.getAnnotation(Table.class);
        if (annot != null && StringUtils.isNotEmpty(annot.name())) {
            return annot.name();
        }
        return pojoType.getSimpleName();
    }

    public static String tableName(Facet facet) {
        Table annot = facet.getAnnotation(Table.class);
        GenericType elementType = getElementType(facet);
        if (annot != null && StringUtils.isNotEmpty(annot.name())) {
            return annot.name();
        }
        return tableName(elementType.getRawType());
    }

    public static String indexName(Facet facet) {
        Index annot = facet.getAnnotation(Index.class);
        if (annot != null && StringUtils.isNotEmpty(annot.name())) {
            return annot.name();
        }
        return StringUtils.EMPTY;
    }

    public static boolean isIndexed(Facet facet) {
        return facet.getAnnotation(Index.class) != null;
    }

    public static int ttl(Class<?> pojoType) {
        Table annot = pojoType.getAnnotation(Table.class);
        if (annot != null) {
            return annot.ttl();
        }
        return 0;
    }

    private static Class<?> typeArgument(DataType dataType, int index) {
        return dataType.getTypeArguments().get(index).asJavaClass();
    }
}

/*
 * Copyright 2014 Savoir Technologies
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

package com.savoirtech.hecate.cql3;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public final class FieldMapper {

    private FieldMapper() {
    }

    private final static Logger logger = LoggerFactory.getLogger(FieldMapper.class);

    private static final Map<String, String> fromCassandra = new HashMap<>();

    private static final Map<String, String> toCassandra = new HashMap<>();

    private static final String TYPE_NAME_PREFIX = "class ";

    static {
        {
            fromCassandra.put("TEXT", String.class.getName());
            fromCassandra.put("VARCHAR", String.class.getName());
            fromCassandra.put("BIGINT", Long.class.getName());
            fromCassandra.put("BOOLEAN", Boolean.class.getName());
            fromCassandra.put("DOUBLE", Double.class.getName());
            fromCassandra.put("FLOAT", Float.class.getName());
            fromCassandra.put("INT", Integer.class.getName());
            fromCassandra.put("UUID", UUID.class.getName());
            fromCassandra.put("TIMESTAMP", Date.class.getName());

            for (Map.Entry<String, String> entry : fromCassandra.entrySet()) {
                toCassandra.put(entry.getValue(), entry.getKey());
            }

            toCassandra.put(int.class.getName(), "INT");
            toCassandra.put(boolean.class.getName(), "BOOLEAN");
            toCassandra.put(float.class.getName(), "FLOAT");
            toCassandra.put(double.class.getName(), "DOUBLE");
            toCassandra.put(long.class.getName(), "BIGINT");
        }
    }

    public static Object getJavaObject(ColumnDefinitions.Definition definition, Row row) {
        final String columnName = definition.getName();
        switch (definition.getType().getName()) {
            case TEXT:
            case VARCHAR:
                return row.getString(columnName);
            case BIGINT:
                return row.getLong(columnName);
            case BOOLEAN:
                return row.getBool(columnName);
            case DOUBLE:
                return row.getDouble(columnName);
            case FLOAT:
                return row.getFloat(columnName);
            case INT:
                return row.getInt(columnName);
            case LIST:
                return row.getList(columnName, Object.class);
            case SET:
                return row.getSet(columnName, Object.class);
            case MAP:
                return row.getMap(columnName, Object.class, Object.class);
            case UUID:
                return row.getUUID(columnName);
            case TIMESTAMP:
                return row.getDate(columnName);
            default:
                return row.getBytes(columnName);

        }
    }

    private static String getClassName(Type type) {
        if (type == null) {
            return "";
        }
        String className = type.toString();
        if (className.startsWith(TYPE_NAME_PREFIX)) {
            className = className.substring(TYPE_NAME_PREFIX.length());
        }
        return className;
    }

    public static String getCassandraType(Field field) {
        String fieldType = toCassandra.get(field.getType().getName());

        if (fieldType != null) {
            return fieldType;
        }

        if (field.getType().isAssignableFrom(List.class)) {

            Type type = field.getGenericType();
            if (type instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) type;

                logger.debug(toCassandra.get(getClassName(pt.getActualTypeArguments()[0])));
                String csType = (StringUtils.isEmpty(toCassandra.get(getClassName(pt.getActualTypeArguments()[0])))) ? "blob" : toCassandra.get(
                        getClassName(pt.getActualTypeArguments()[0]));
                return "list<" + csType + ">";
            }

            return "list<blob>";
        }

        if (field.getType().isAssignableFrom(Set.class)) {

            Type type = field.getGenericType();
            if (type instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) type;
                return "set<" + toCassandra.get(getClassName(pt.getActualTypeArguments()[0])) + ">";
            }

            return "set<blob>";
        }

        if (field.getType().isAssignableFrom(Map.class))

        {
            Type type = field.getGenericType();
            if (type instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) type;
                return "map<" + toCassandra.get(getClassName(pt.getActualTypeArguments()[0])) + "," +
                        toCassandra.get(getClassName(pt.getActualTypeArguments()[1])) + ">";
            }

            return "map<blob,blob>";
        }

        return "blob";
    }
}

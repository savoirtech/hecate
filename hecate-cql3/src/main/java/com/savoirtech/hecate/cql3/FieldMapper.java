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

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FieldMapper {

    private FieldMapper() {}

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

    public static Object getJavaObject(String type, String column, Row row) {

        if ("TEXT".equalsIgnoreCase(type)) {
            return row.getString(column);
        }

        if ("VARCHAR".equalsIgnoreCase(type)) {
            return row.getString(column);
        }

        if ("BIGINT".equalsIgnoreCase(type)) {
            return row.getLong(column);
        }

        if ("BOOLEAN".equalsIgnoreCase(type)) {
            return row.getBool(column);
        }

        if ("DOUBLE".equalsIgnoreCase(type)) {
            return row.getDouble(column);
        }

        if ("FLOAT".equalsIgnoreCase(type)) {
            return row.getFloat(column);
        }

        if ("INT".equalsIgnoreCase(type)) {
            return row.getInt(column);
        }

        if (type.startsWith("LIST")) {

            return row.getList(column, Object.class);
        }

        if (type.startsWith("SET")) {
            return row.getSet(column, Object.class);
        }

        if (type.startsWith("MAP")) {
            return row.getMap(column, Object.class, Object.class);
        }

        if ("UUID".equalsIgnoreCase(type)) {
            return row.getUUID(column);
        }

        if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return row.getDate(column);
        }

        return row.getBytes(column);
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
                return "list<" + toCassandra.get(getClassName(pt.getActualTypeArguments()[0])) + ">";
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

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

package com.savoirtech.hecate.cql3;

import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.exception.HecateException;
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

    public static final Map<String, String> fromCassandra = new HashMap<>();

    public static final Map<String, String> toCassandra = new HashMap<>();

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

    public static String getCassandraTypeForFieldName(String field, Class cls) throws HecateException {
        for (Field f : ReflectionUtils.getFieldsUpTo(cls, null)) {
            if (field.equals(f.getName())) {
                return getCassandraType(f);
            }
        }
        return null;
    }

    public static String getCassandraType(Field field) throws HecateException {
        String fieldType = toCassandra.get(field.getType().getName());

        if (fieldType != null) {
            return fieldType;
        }

        if (field.getType().isAssignableFrom(List.class)) {

            Type type = field.getGenericType();
            if (type instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) type;
                String csType = getStorageType(pt.getActualTypeArguments()[0]);
                if (!csType.equals("blob")) {
                    return "list<" + csType + ">";
                } else {

                    try {
                        return "list<" + FieldMapper.getCassandraTypeForFieldName(ReflectionUtils.getIdName(Class.forName(getClassName(
                                pt.getActualTypeArguments()[0]))), Class.forName(getClassName(pt.getActualTypeArguments()[0]))) + ">";
                    }
                    catch (ClassNotFoundException e) {
                        logger.error("Class error " + e);
                    }
                }
            }

            return "list<blob>";
        }

        if (field.getType().isAssignableFrom(Set.class)) {

            Type type = field.getGenericType();
            if (type instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) type;
                String csType = getStorageType(pt.getActualTypeArguments()[0]);

                if (!csType.equals("blob")) {
                    return "set<" + csType + ">";
                } else {

                    try {
                        return "set<" + FieldMapper.getCassandraTypeForFieldName(ReflectionUtils.getIdName(Class.forName(getClassName(
                                pt.getActualTypeArguments()[0]))), Class.forName(getClassName(pt.getActualTypeArguments()[0]))) + ">";
                    }
                    catch (ClassNotFoundException e) {
                        logger.error("Class error " + e);
                    }
                }
            }

            return "set<blob>";
        }

        if (field.getType().isAssignableFrom(Map.class)) {
            Type type = field.getGenericType();
            if (type instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) type;
                if ("blob".equals(getStorageType(pt.getActualTypeArguments()[0]))) {
                    throw new HecateException("Complex keys not supported");
                }
                if ("blob".equals(getStorageType(pt.getActualTypeArguments()[1]))) {
                    try {
                        return "map<" + getStorageType(pt.getActualTypeArguments()[0]) + "," +
                                FieldMapper.getCassandraTypeForFieldName(ReflectionUtils.getIdName(Class.forName(getClassName(
                                        pt.getActualTypeArguments()[1]))), Class.forName(getClassName(pt.getActualTypeArguments()[1]))) +
                                ">";
                    }
                    catch (ClassNotFoundException e) {
                        logger.error("Class error " + e);
                    }
                }
                return "map<" + getStorageType(pt.getActualTypeArguments()[0]) + "," +
                        getStorageType(pt.getActualTypeArguments()[1]) + ">";
            }

            return "map<blob,blob>";
        }

        System.out.println("Field " + field.getName() + " maps as " + FieldMapper.getCassandraTypeForFieldName(field.getName(), field.getType()));

        return FieldMapper.getCassandraTypeForFieldName(ReflectionUtils.getIdName(field.getType()), field.getType());
    }

    public static String getRawCassandraType(Field field) throws HecateException {
        String fieldType = toCassandra.get(field.getType().getName());

        if (fieldType != null) {
            return fieldType;
        }

        if (field.getType().isAssignableFrom(List.class)) {

            Type type = field.getGenericType();
            if (type instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) type;
                String csType = getStorageType(pt.getActualTypeArguments()[0]);
                if (!csType.equals("blob")) {
                    return "list<" + csType + ">";
                }
            }

            return "list<blob>";
        }

        if (field.getType().isAssignableFrom(Set.class)) {

            Type type = field.getGenericType();
            if (type instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) type;
                String csType = getStorageType(pt.getActualTypeArguments()[0]);

                if (!csType.equals("blob")) {
                    return "set<" + csType + ">";
                }
            }

            return "set<blob>";
        }

        if (field.getType().isAssignableFrom(Map.class)) {
            Type type = field.getGenericType();
            if (type instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) type;
                if ("blob".equals(getStorageType(pt.getActualTypeArguments()[0]))) {
                    throw new HecateException("Complex keys not supported");
                }

                return "map<" + getStorageType(pt.getActualTypeArguments()[0]) + "," +
                        getStorageType(pt.getActualTypeArguments()[1]) + ">";
            }

            return "map<blob,blob>";
        }
        //We have an Object, we need to covert the Object's Cassandra ID to a Key value
        //create a new Table Statement and then add this entity.

        return FieldMapper.getRawCassandraTypeForFieldName(ReflectionUtils.getIdName(field.getType()), field.getType());
    }


    private static String getRawCassandraTypeForFieldName(String idName, Class<?> type) throws HecateException {
        for (Field f : ReflectionUtils.getFieldsUpTo(type, null)) {
            if (type.equals(f.getName())) {
                return getRawCassandraType(f);
            }
        }
        return null;
    }

    private static String getStorageType(Type type) {

        String csClass = toCassandra.get(getClassName(type));
        if (csClass != null && !StringUtils.isEmpty(csClass)) {
            return csClass;
        }
        //This is actually an Object.
        //Figure out what the Id Key is and how it'll be used.
        return "blob";
    }
}

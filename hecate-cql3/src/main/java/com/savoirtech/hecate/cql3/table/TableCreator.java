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

package com.savoirtech.hecate.cql3.table;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.savoirtech.hecate.cql3.FieldMapper;
import com.savoirtech.hecate.cql3.HecateException;
import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.annotations.IdColumn;
import com.savoirtech.hecate.cql3.annotations.PrimaryKey;
import org.apache.commons.lang3.StringUtils;

public class TableCreator {

    private TableCreator() {}

    public static List<TableDescriptor> createPojoTables(String keySpace, String tableName, Class cls) {

        List<TableDescriptor> descriptors = new ArrayList<>();

        return descriptors;
    }

    public static String createTable(String keySpace, String tableName, Class cls) throws HecateException {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE IF NOT EXISTS ");
        builder.append(keySpace);
        builder.append(".");
        builder.append(tableName);
        builder.append(" ( ");
        boolean idFound = false;

        //Look for compound primary key annotation.
        String primaryKey = null;
        if (cls.isAnnotationPresent(PrimaryKey.class)) {
            PrimaryKey p = (PrimaryKey) cls.getAnnotation(PrimaryKey.class);
            primaryKey = p.pk();
            idFound = true;
        }
        StringBuilder flds = new StringBuilder();
        for (Field field : ReflectionUtils.getFieldsUpTo(cls, null)) {
            if (flds.length() > 0) {
                flds.append(",");
            }

            flds.append(field.getName());
            flds.append(" ");

            String csType = FieldMapper.getRawCassandraType(field);

            if (StringUtils.isEmpty(csType)) {
                flds.append("blob");
            } else {
                flds.append(FieldMapper.getRawCassandraType(field));
            }

            if (field.isAnnotationPresent(IdColumn.class)) {
                if (idFound) {
                    throw new HecateException("Cannot have multiple PRIMARY KEY declarations");
                } else {
                    //Idcolumn

                    flds.append(" PRIMARY KEY");
                    idFound = true;
                }
            }
        }

        builder.append(flds);

        if (primaryKey != null) {

            builder.append(",");
            builder.append(" PRIMARY KEY (");
            List<String> pkl = Arrays.asList(primaryKey.split(","));
            StringBuilder keyBuilder = new StringBuilder();
            for (String key : pkl) {
                if (keyBuilder.length() > 0) {
                    keyBuilder.append(",");
                }
                keyBuilder.append(key.trim());
            }

            builder.append(keyBuilder);
            builder.append(")");
        }
        builder.append(" );");

        return builder.toString();
    }

    public static String createNestedTables(Map<String, StringBuilder> tables, String keySpace, String tableName, Class cls) throws HecateException {

        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE IF NOT EXISTS ");
        builder.append(keySpace);
        builder.append(".");
        builder.append(tableName);
        builder.append(" ( ");
        boolean idFound = false;

        //Look for compound primary key annotation.
        String primaryKey = null;
        if (cls.isAnnotationPresent(PrimaryKey.class)) {
            PrimaryKey p = (PrimaryKey) cls.getAnnotation(PrimaryKey.class);
            primaryKey = p.pk();
            idFound = true;
        }
        StringBuilder flds = new StringBuilder();
        for (Field field : ReflectionUtils.getFieldsUpTo(cls, null)) {
            if (flds.length() > 0) {
                flds.append(",");
            }

            flds.append(field.getName());
            flds.append(" ");

            String csType = FieldMapper.getRawCassandraType(field);
            if (csType == null) {
                field.getType();
                createNestedTables(tables, keySpace, ReflectionUtils.tableName(field), field.getType());
            }

            if (csType != null && csType.toLowerCase().startsWith("list<blob")) {

                Type type = field.getGenericType();
                if (type instanceof ParameterizedType) {
                    ParameterizedType pt = (ParameterizedType) type;

                    Class clazz = null;
                    try {
                        clazz = ReflectionUtils.typeToClass(pt.getActualTypeArguments()[0],field.getDeclaringClass().getClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new HecateException(e);
                    }
                    createNestedTables(tables, keySpace, ReflectionUtils.tableName(field), clazz);
                }
            }

            if (csType != null && csType.toLowerCase().startsWith("set<blob")) {
                Type type = field.getGenericType();
                if (type instanceof ParameterizedType) {
                    ParameterizedType pt = (ParameterizedType) type;

                    Class clazz = null;
                    try {
                        clazz = ReflectionUtils.typeToClass(pt.getActualTypeArguments()[0],field.getDeclaringClass().getClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new HecateException(e);
                    }

                    createNestedTables(tables, keySpace, ReflectionUtils.tableName(field), clazz);
                }
            }

            if (csType != null && csType.toLowerCase().contains("map<") && csType.toLowerCase().contains(",blob>")) {
                Type type = field.getGenericType();
                if (type instanceof ParameterizedType) {
                    ParameterizedType pt = (ParameterizedType) type;

                    Class clazz = null;
                    try {
                        clazz = ReflectionUtils.typeToClass(pt.getActualTypeArguments()[1],field.getDeclaringClass().getClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new HecateException(e);
                    }

                    createNestedTables(tables, keySpace, ReflectionUtils.tableName(field), clazz);
                }
            }

            flds.append(FieldMapper.getCassandraType(field));

            if (field.isAnnotationPresent(IdColumn.class)) {
                if (idFound) {
                    throw new HecateException("Cannot have multiple PRIMARY KEY declarations");
                } else {
                    //Idcolumn

                    flds.append(" PRIMARY KEY");
                    idFound = true;
                }
            }
        }

        builder.append(flds);

        if (primaryKey != null) {

            builder.append(",");
            builder.append(" PRIMARY KEY (");
            List<String> pkl = Arrays.asList(primaryKey.split(","));
            StringBuilder keyBuilder = new StringBuilder();
            for (String key : pkl) {
                if (keyBuilder.length() > 0) {
                    keyBuilder.append(",");
                }
                keyBuilder.append(key.trim());
            }

            builder.append(keyBuilder);
            builder.append(")");
        }
        builder.append(" );");

        tables.put(tableName, builder);
        return builder.toString();
    }
}

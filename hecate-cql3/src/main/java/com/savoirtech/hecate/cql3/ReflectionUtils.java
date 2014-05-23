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
import com.google.common.collect.Lists;
import com.savoirtech.hecate.cql3.annotations.IdColumn;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ReflectionUtils {

    private final static Logger logger = LoggerFactory.getLogger(ReflectionUtils.class);

    public static <K> String getIdName(Class clazz) {
        Map<String,Field> fieldsMap = fieldsMap(clazz);
        for (Field field : fieldsMap.values()) {
            if (field.isAnnotationPresent(IdColumn.class)) {
                return field.getName();
            }
        }
        return null;
    }

    public static Field getFieldType(String id) {
        return null;
    }

    public static <K> K extractFieldValue(String fieldName, Field fieldType, Row row) {
        return null;
    }

    public static <T> Object[] fieldValues(T pojo) {
        Map<String, Field> fieldsMap = fieldsMap(pojo.getClass());
        List<Object> vals = new ArrayList<>();
        for (Map.Entry<String, Field> entry : fieldsMap.entrySet()) {
            final Field field = entry.getValue();
            field.setAccessible(true);
            try {
                vals.add(field.get(pojo));
            }
            catch (IllegalAccessException e) {
                logger.error("Could not access field.", e);
                vals.add(null);
            }
        }
        return vals.toArray(new Object[vals.size()]);
    }

    public static String[] fieldNames(Class mappingClazz) {
        List<String> fields = new ArrayList<>();
        final Map<String,Field> fieldsMap = fieldsMap(mappingClazz);
        for (Field field : fieldsMap.values()) {
            fields.add(field.getName());
        }
        return fields.toArray(new String[fields.size()]);
    }

    private static boolean isPersistable(Field field) {
        final int mods = field.getModifiers();
        return !(Modifier.isFinal(mods) || Modifier.isStatic(mods) || Modifier.isTransient(mods));
    }

    private static void collectFields(Map<String, Field> fieldsMap, Class<?> type) {
        for (Field field : type.getDeclaredFields()) {
            if (isPersistable(field)) {
                fieldsMap.put(field.getName().toUpperCase(), field);
            }
        }
        final Class<?> superclass = type.getSuperclass();
        if (superclass != null) {
            collectFields(fieldsMap, superclass);
        }
    }

    public static Map<String, Field> fieldsMap(Class<?> type) {
        final Map<String, Field> fieldsMap = new TreeMap<>();
        collectFields(fieldsMap, type);
        return fieldsMap;
    }

    public static <T> void populate(T clz, Row row) {
        final Map<String, Field> fieldsMap = fieldsMap(clz.getClass());
        for (ColumnDefinitions.Definition cf : row.getColumnDefinitions()) {
            logger.debug("Column " + cf.getType().asJavaClass());
            final Field field = fieldsMap.get(cf.getName().toUpperCase());
            if (field != null) {
                field.setAccessible(true);
                try {
                    field.set(clz, FieldMapper.getJavaObject(cf, row));
                }
                catch (IllegalAccessException e) {
                    logger.error("Access problem", e);
                }
            } else {
                logger.error("Unable to find field matching column {}.", cf.getName());
            }
        }
    }

    /**
     *
     // convert object to bytes
     Date d1 = new Date();
     System.out.println(d1);
     ByteArrayOutputStream baos = new ByteArrayOutputStream();
     ObjectOutputStream oos = new ObjectOutputStream(baos);
     oos.writeObject(d1);
     byte[] buf = baos.toByteArray();

     // convert back from bytes to object
     ObjectInputStream ois =
     new ObjectInputStream(new ByteArrayInputStream(buf));
     Date d2 = (Date) ois.readObject();
     ois.close();

     System.out.println(d2);
     } catch (IOException ioe) {
     ioe.printStackTrace();
     } catch (ClassNotFoundException cnfe) {
     cnfe.printStackTrace();
     }
     */
}

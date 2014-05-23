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
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.savoirtech.hecate.cql3.annotations.IdColumn;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ReflectionUtils {

    private final static Logger logger = LoggerFactory.getLogger(ReflectionUtils.class);

    public static <K> String getIdName(Class clazz) {
        Map<String, Field> fieldsMap = columnNameToField(clazz);
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
        Map<String, Field> fieldsMap = columnNameToField(pojo.getClass());
        List<Object> vals = new ArrayList<>();
        for (String columnName : fieldsMap.keySet()) {
            final Field field = fieldsMap.get(columnName);
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

    public static String[] columnNames(Class mappingClazz) {
        List<String> fields = new ArrayList<>();
        final Map<String, Field> fieldsMap = columnNameToField(mappingClazz);
        for (String columnName : fieldsMap.keySet()) {
            fields.add(columnName);
        }
        return fields.toArray(new String[fields.size()]);
    }

    private static boolean isPersistable(Field field) {
        final int mods = field.getModifiers();
        return !(Modifier.isFinal(mods) || Modifier.isStatic(mods) || Modifier.isTransient(mods));
    }

    private static void collectFields(List<Field> fields, Class<?> type) {
        for (Field field : type.getDeclaredFields()) {
            if (isPersistable(field)) {
                fields.add(field);
            }
        }
        final Class<?> superclass = type.getSuperclass();
        if (superclass != null) {
            collectFields(fields, superclass);
        }
    }

    private static final Comparator<Field> FIELD_COMPARATOR = new FieldComparator();

    private static final class FieldComparator implements Comparator<Field> {
        @Override
        public int compare(Field o1, Field o2) {
            return o1.toGenericString().compareTo(o2.toGenericString());
        }


    }

    private static List<Field> getFields(Class<?> type) {
        List<Field> fields = new LinkedList<>();
        collectFields(fields, type);
        return fields;
    }

    private static Multimap<String, Field> fieldsMap(Class<?> type) {
        final Multimap<String, Field> fieldsMap = TreeMultimap.create(Ordering.natural(), FIELD_COMPARATOR);
        for (Field field : getFields(type)) {
            fieldsMap.put(field.getName(), field);
        }
        return fieldsMap;
    }

    public static Map<String, Field> columnNameToField(Class<?> type) {
        final Multimap<String, Field> fieldsMap = fieldsMap(type);
        final Map<String,Field> columnNameToField = new TreeMap<>();
        for (String fieldName : fieldsMap.keySet()) {
            final Collection<Field> fields = fieldsMap.get(fieldName);
            if (fields.size() == 1) {
                columnNameToField.put(StringUtils.lowerCase(fieldName), fields.iterator().next());
            } else {
                for (Field field : fields) {
                    columnNameToField.put(StringUtils.lowerCase(field.getDeclaringClass().getSimpleName() + "_" + field.getName()), field);
                }
            }
        }
        return columnNameToField;
    }

    public static <T> void populate(T clz, Row row) {
        final Map<String, Field> fieldsMap = columnNameToField(clz.getClass());
        for (ColumnDefinitions.Definition cf : row.getColumnDefinitions()) {
            logger.debug("Column " + cf.getType().asJavaClass());
            final Field field = fieldsMap.get(cf.getName());
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

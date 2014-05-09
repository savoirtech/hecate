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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import com.savoirtech.hecate.cql3.annotations.IdColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectionUtils {

    private final static Logger logger = LoggerFactory.getLogger(ReflectionUtils.class);

    public static <K> String getIdName(Class clazz) {

        for (Field fied : getFieldsUpTo(clazz, null)) {

            if (fied.isAnnotationPresent(IdColumn.class)) {
                return fied.getName();
            }
        }

        return null;
    }

    public static Field getFieldType(String id) {return null;}

    public static <K> K extractFieldValue(String fieldName, Field fieldType, Row row) {return null;}

    public static <T> Object[] fieldValues(T pojo) {
        List vals = new ArrayList();
        for (Field field : getFieldsUpTo(pojo.getClass(), null)) {
            try {
                field.setAccessible(true);
                Object value = field.get(pojo);
                vals.add(value);
            } catch (IllegalAccessException e) {
                logger.error("Could not access field " + e);
            }
        }
        return vals.toArray(new Object[vals.size()]);
    }

    public static String[] fieldNames(Class mappingClazz) {
        List<String> fields = new ArrayList<>();
        for (Field field : getFieldsUpTo(mappingClazz, null)) {
            fields.add(field.getName());
        }
        return fields.toArray(new String[fields.size()]);
    }

    @SuppressWarnings("unchecked")
    static <T> T[] newArray(Class<T> type, int length) {
        return (T[]) Array.newInstance(type, length);
    }

    public static Iterable<Field> getFieldsUpTo(Class<?> startClass, Class<?> exclusiveParent) {

        List<Field> currentClassFields = Lists.newArrayList(startClass.getDeclaredFields());
        Class<?> parentClass = startClass.getSuperclass();

        if (parentClass != null && (exclusiveParent == null || !(parentClass.equals(exclusiveParent)))) {
            List<Field> parentClassFields = (List<Field>) getFieldsUpTo(parentClass, exclusiveParent);
            currentClassFields.addAll(parentClassFields);
        }

        return currentClassFields;
    }

    public static <T> void populate(T clz, Row row) {

        for (ColumnDefinitions.Definition cf : row.getColumnDefinitions()) {
            logger.debug("Column " + cf.getType().asJavaClass());

            List<String> fields = Arrays.asList(fieldNames(clz.getClass()));
            try {
                for (String fname : fields) {
                    if (fname.equalsIgnoreCase(cf.getName())) {
                        Field field = clz.getClass().getDeclaredField(fname);
                        field.setAccessible(true);
                        field.set(clz, FieldMapper.getJavaObject(cf.getType().getName().name(), cf.getName(), row));
                    }
                }
            } catch (NoSuchFieldException e) {
                logger.error("Trying to access a field that doesn't exist " + e);
            } catch (IllegalAccessException e) {
                logger.error("Access problem " + e);
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

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

package com.savoirtech.hecate.core.config;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.savoirtech.hecate.core.annotations.CassandraCollection;
import com.savoirtech.hecate.core.annotations.CassandraMaps;
import com.savoirtech.hecate.core.utils.CassandraAnnotationLogic;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import org.apache.commons.lang.StringUtils;

public final class HectorHelper {

    /**
     * Instantiates a new hector helper.
     */
    private HectorHelper() {
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

    public static String getMaxUTF8Value() {
        return "\u10ffff";
    }

    public static java.util.UUID getMinTimeUUID() {
        return TimeUUIDUtils.getTimeUUID(0);
    }

    public static java.util.UUID getMaxTimeUUID() {
        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.set(3000, Calendar.DECEMBER, 31, 23, 59, 59);
        return TimeUUIDUtils.getTimeUUID(cal.getTimeInMillis());
    }

    /**
     * Gets the time uuid.
     *
     * @return the time uuid
     */
    public static java.util.UUID getTimeUUID() {
        return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
    }

    public static BigInteger getMD5(String theString) throws Exception {
        byte[] bytesOfMessage = theString.getBytes("UTF-8");

        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] thedigest = md.digest(bytesOfMessage);

        BigInteger bigInt = new BigInteger(1, thedigest);

        return bigInt;
    }

    public static int compareStringMD5(String string1, String string2) {

        BigInteger md51;
        BigInteger md52;
        try {
            md51 = getMD5(string1);
            md52 = getMD5(string2);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return md51.compareTo(md52);
    }

    /**
     * As byte array.
     *
     * @param uuid the uuid
     * @return the byte[]
     */
    public static byte[] asByteArray(java.util.UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        byte[] buffer = new byte[16];

        for (int i = 0;i < 8;i++) {
            buffer[i] = (byte) (msb >>> 8 * (7 - i));
        }
        for (int i = 8;i < 16;i++) {
            buffer[i] = (byte) (lsb >>> 8 * (7 - i));
        }

        return buffer;
    }

    /**
     * Gets the columns.
     *
     * @param <T>    the generic type
     * @param entity the entity
     * @return the columns
     */
    public static <T> List<HColumn<String, ?>> getColumns(T entity) {
        try {
            List<HColumn<String, ?>> columns = new ArrayList<HColumn<String, ?>>();
            Iterable<Field> fields = getFieldsUpTo(entity.getClass(), null);
            // entity.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object value = field.get(entity);

                if (value == null) {
                    // Field has no value so nothing to store
                    continue;
                }

                String name = field.getName();

                Serializer ser = SerializerTypeInferer.getSerializer(value);

                //If it cannot figure out which serializer, then just use an ObjectSerializer
                if (ser == null) {
                    ser = ObjectSerializer.get();
                }
                HColumn<String, ?> column = HFactory.createColumn(name, value, StringSerializer.get(), ser);

                columns.add(column);
            }
            return columns;
        } catch (Exception e) {
            throw new RuntimeException("Reflection exception", e);
        }
    }

    /**
     * Gets the columns.
     *
     * @param <T>    the generic type
     * @param entity the entity
     * @return the columns
     */
    public static <T> List<HColumn<String, ?>> getColumnsAndAnnotations(T entity) {
        try {
            List<HColumn<String, ?>> columns = new ArrayList<HColumn<String, ?>>();
            Iterable<Field> fields = getFieldsUpTo(entity.getClass(), null);
            // Field[] fields = entity.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object value = field.get(entity);

                if (value == null) {
                    // Field has no value so nothing to store
                    continue;
                }

                if (value instanceof Collection && field.getAnnotation(CassandraCollection.class) != null) {
                    Collection list = (Collection) value;

                    Integer counter = 0;
                    for (Object o : list) {
                        HColumn<String, ?> column = HFactory.createColumn(field.getName() + CassandraAnnotationLogic.LIST_PREFIX + counter,
                            (String) o, StringSerializer.get(), StringSerializer.get());
                        columns.add(column);
                        counter++;
                    }
                    //We have filled the collection.
                    continue;
                }

                if (value instanceof Map && field.getAnnotation(CassandraMaps.class) != null) {

                    Map map = (Map) value;
                    for (Object mapkey : map.keySet()) {
                        if (!(mapkey instanceof String)) {
                            throw new RuntimeException("Cannot handle non string keys");
                        }

                        if (map.get(mapkey) instanceof String) {
                            HColumn<String, ?> column = HFactory.createColumn(field.getName() + CassandraAnnotationLogic.MAP_PREFIX + mapkey,
                                (String) map.get(
                                    //mapkey), StringSerializer.get(), SerializerTypeInferer.getSerializer(map.get(mapkey)));
                                    mapkey), StringSerializer.get(), StringSerializer.get());
                            columns.add(column);
                        } else {
                            if (map.get(mapkey) instanceof Collection) {
                                List<String> values = (List) map.get(mapkey);
                                String mapValue = CassandraAnnotationLogic.RECORD_LIST_START + StringUtils.join(values,
                                    CassandraAnnotationLogic.RECORD_LIST_DELIMITER);

                                HColumn<String, ?> column = HFactory.createColumn(field.getName() + CassandraAnnotationLogic.MAP_PREFIX + mapkey,
                                    mapValue, StringSerializer.get(), StringSerializer.get());

                                columns.add(column);
                            }
                        }
                    }
                    //We have filled the map
                    continue;
                }

                String name = field.getName();

                Serializer ser = SerializerTypeInferer.getSerializer(value);
                //If it cannot figure out which serializer, then just use an ObjectSerializer
                if (ser == null) {
                    ser = ObjectSerializer.get();
                }
                HColumn<String, ?> column = HFactory.createColumn(name, value, StringSerializer.get(), ser);

                columns.add(column);
            }
            return columns;
        } catch (Exception e) {
            throw new RuntimeException("Reflection exception", e);
        }
    }

    /**
     * Gets the columns.
     *
     * @param <T>    the generic type
     * @param entity the entity
     * @return the columns
     */
    public static <T> List<HColumn<String, ?>> getColumnsNoAnnotations(T entity) {
        try {
            List<HColumn<String, ?>> columns = new ArrayList<HColumn<String, ?>>();
            Iterable<Field> fields = getFieldsUpTo(entity.getClass(), null);
            // Field[] fields = entity.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object value = field.get(entity);

                if (value == null) {
                    // Field has no value so nothing to store
                    continue;
                }

                if (value instanceof Collection) {
                    Collection list = (Collection) value;

                    Integer counter = 0;
                    for (Object o : list) {
                        HColumn<String, ?> column = HFactory.createColumn(field.getName() + CassandraAnnotationLogic.LIST_PREFIX + counter,
                            (String) o, StringSerializer.get(), StringSerializer.get());
                        columns.add(column);
                        counter++;
                    }
                    //We have filled the collection.
                    continue;
                }

                if (value instanceof Map) {

                    Map map = (Map) value;
                    for (Object mapkey : map.keySet()) {
                        if (!(mapkey instanceof String)) {
                            throw new RuntimeException("Cannot handle non string keys");
                        }

                        if (map.get(mapkey) instanceof String) {
                            HColumn<String, ?> column = HFactory.createColumn(field.getName() + CassandraAnnotationLogic.MAP_PREFIX + mapkey,
                                (String) map.get(
                                    //mapkey), StringSerializer.get(), SerializerTypeInferer.getSerializer(map.get(mapkey)));
                                    mapkey), StringSerializer.get(), StringSerializer.get());
                            columns.add(column);
                        } else {
                            if (map.get(mapkey) instanceof Collection) {
                                List<String> values = (List) map.get(mapkey);
                                String mapValue = CassandraAnnotationLogic.RECORD_LIST_START + StringUtils.join(values,
                                    CassandraAnnotationLogic.RECORD_LIST_DELIMITER);

                                HColumn<String, ?> column = HFactory.createColumn(field.getName() + CassandraAnnotationLogic.MAP_PREFIX + mapkey,
                                    mapValue, StringSerializer.get(), StringSerializer.get());

                                columns.add(column);
                            }
                        }
                    }
                    //We have filled the map
                    continue;
                }

                String name = field.getName();

                Serializer ser = SerializerTypeInferer.getSerializer(value);
                //If it cannot figure out which serializer, then just use an ObjectSerializer
                if (ser == null) {
                    ser = ObjectSerializer.get();
                }
                HColumn<String, ?> column = HFactory.createColumn(name, value, StringSerializer.get(), ser);

                columns.add(column);
            }
            return columns;
        } catch (Exception e) {
            throw new RuntimeException("Reflection exception", e);
        }
    }

    public static <T> List<HColumn<String, Object>> getObjectColumns(T entity) {
        try {
            List<HColumn<String, Object>> columns = new ArrayList<HColumn<String, Object>>();
            Iterable<Field> fields = getFieldsUpTo(entity.getClass(), null);
            //Field[] fields = entity.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object value = field.get(entity);

                if (value == null) {
                    // Field has no value so nothing to store
                    continue;
                }

                String name = field.getName();
                Serializer ser = SerializerTypeInferer.getSerializer(value);
                HColumn<String, Object> column = HFactory.createColumn(name, value, StringSerializer.get(), ser);

                columns.add(column);
            }
            return columns;
        } catch (Exception e) {
            throw new RuntimeException("Reflection exception", e);
        }
    }

    /**
     * Gets the string cols.
     *
     * @param <T>    the generic type
     * @param entity the entity
     * @return the string cols
     */
    public static <T> List<HColumn<String, String>> getStringCols(T entity) {
        try {
            List<HColumn<String, ?>> cols = getColumns(entity);
            List<HColumn<String, String>> retCols = new ArrayList<HColumn<String, String>>();

            for (HColumn<String, ?> col : cols) {
                retCols.add(HFactory.createStringColumn(col.getName(), col.getValue().toString()));
            }

            return retCols;
        } catch (Exception e) {
            throw new RuntimeException("Reflection away", e);
        }
    }

    /**
     * Populate entity.
     *
     * @param <T>    the generic type
     * @param t      the t
     * @param result the result
     */
    public static <T> void populateEntityAnnotated(T t, QueryResult<ColumnSlice<String, byte[]>> result) {
        try {
            Iterable<Field> fields = getFieldsUpTo(t.getClass(), null);
            //Field[] fields = t.getClass().getDeclaredFields();
            for (Field field : fields) {
                //If the field is final (likely a serialVersionUID), skip it because we can't set it
                if (Modifier.isFinal(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                String name = field.getName();
                //TODO, this is expensive as we have to traverse the columns.

                if (field.getType().isAssignableFrom(Map.class)) {
                    Map map = new HashMap<>();
                    for (HColumn<String, byte[]> mapCo : result.get().getColumns()) {

                        if (mapCo.getName().startsWith(field.getName() + CassandraAnnotationLogic.MAP_PREFIX)) {
                            String key = mapCo.getName().replaceAll(field.getName() + CassandraAnnotationLogic.MAP_PREFIX, "");

                            Object val = StringSerializer.get().fromBytes(mapCo.getValue());

                            String checkValue = (String) val;
                            if (checkValue != null && checkValue.startsWith(CassandraAnnotationLogic.RECORD_LIST_START)) {
                                checkValue = checkValue.replaceFirst(CassandraAnnotationLogic.RECORD_LIST_START, "");
                                List values = Arrays.asList(checkValue.split(CassandraAnnotationLogic.RECORD_LIST_DELIMITER));
                                map.put(key, values);
                            } else {

                                map.put(key, val);
                            }
                        }
                    }

                    field.set(t, map);
                    continue;
                }

                if (field.getType().isAssignableFrom(Set.class)) {

                    Set list = new HashSet();
                    for (HColumn<String, byte[]> lisCo : result.get().getColumns()) {

                        if (lisCo.getName().startsWith(field.getName() + CassandraAnnotationLogic.LIST_PREFIX)) {

                            Object val = StringSerializer.get().fromBytes(lisCo.getValue());

                            list.add(val);
                        }
                    }

                    field.set(t, list);
                    continue;
                }

                if (field.getType().isAssignableFrom(List.class)) {

                    List list = new ArrayList<>();
                    for (HColumn<String, byte[]> lisCo : result.get().getColumns()) {

                        if (lisCo.getName().startsWith(field.getName() + CassandraAnnotationLogic.LIST_PREFIX)) {

                            Object val = StringSerializer.get().fromBytes(lisCo.getValue());

                            list.add(val);
                        }
                    }

                    field.set(t, list);
                    continue;
                }

                HColumn<String, byte[]> col = result.get().getColumnByName(name);
                if (col == null || col.getValue() == null || col.getValueBytes().capacity() == 0 || col.getValue().length == 0) {
                    // No data for this col
                    continue;
                }

                Object val = SerializerTypeInferer.getSerializer(field.getType()).fromBytes(col.getValue());
                field.set(t, val);
            }
        } catch (IllegalAccessException e) {
            throw new ObjectNotSerializableException("Reflection Error ", e);
        }
    }

    /**
     * Populate entity.
     *
     * @param <T>    the generic type
     * @param t      the t
     * @param result the result
     */
    public static <T> void populateEntity(T t, QueryResult<ColumnSlice<String, byte[]>> result) {
        try {
            Iterable<Field> fields = getFieldsUpTo(t.getClass(), null);
            //Field[] fields = t.getClass().getDeclaredFields();
            for (Field field : fields) {
                //If the field is final (likely a serialVersionUID), skip it because we can't set it
                if (Modifier.isFinal(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                String name = field.getName();

                HColumn<String, byte[]> col = result.get().getColumnByName(name);
                if (col == null || col.getValue() == null || col.getValueBytes().capacity() == 0 || col.getValue().length == 0) {
                    // No data for this col
                    continue;
                }

                Object val = SerializerTypeInferer.getSerializer(field.getType()).fromBytes(col.getValue());
                field.set(t, val);
            }
        } catch (IllegalAccessException e) {
            throw new ObjectNotSerializableException("Reflection Error ", e);
        }
    }

    /**
     * Populate entity.
     *
     * @param <T>    the generic type
     * @param t      the t
     * @param result the result
     */
    public static <T> void populateEntity(T t, ColumnSlice<String, byte[]> result) {
        try {
            Iterable<Field> fields = getFieldsUpTo(t.getClass(), null);
            // Field[] fields = t.getClass().getDeclaredFields();
            for (Field field : fields) {
                //If the field is final (likely a serialVersionUID), skip it because we can't set it
                if (Modifier.isFinal(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                String name = field.getName();

                HColumn<String, byte[]> col = result.getColumnByName(name);
                if (col == null || col.getValue() == null || col.getValueBytes().capacity() == 0) {
                    // No data for this col
                    continue;
                }

                Object val = SerializerTypeInferer.getSerializer(field.getType()).fromBytes(col.getValue());
                field.set(t, val);
            }
        } catch (IllegalAccessException e) {
            throw new ObjectNotSerializableException("Reflection Error ", e);
        }
    }

    public static <ST, T, SKeyType> void populateSuperEntity(ST st, T t, SKeyType superKey, List<HSuperColumn<Object, String, byte[]>> result) {
        try {
            // load key for ST
            // Assumes a Map for columns and 1 other field for key
            Iterable<Field> superFields = getFieldsUpTo(st.getClass(), null);
            // Field[] superFields = st.getClass().getDeclaredFields();
            for (Field superField : superFields) {
                superField.setAccessible(true);
                // the key
                if (!superField.getType().equals(Map.class)) {

                    superField.set(st, superKey);
                } else {
                    // load columns: T
                    HashMap<String, T> columns = new HashMap<String, T>();

                    Field[] fields = t.getClass().getDeclaredFields();
                    for (Field field : fields) {
                        field.setAccessible(true);

                        for (HSuperColumn<Object, String, byte[]> superCol : result) {
                            List<HColumn<String, byte[]>> cols = superCol.getColumns();

                            for (HColumn<String, byte[]> col : cols) {
                                if (col == null || col.getValue() == null || col.getValueBytes().capacity() == 0) {
                                    // No data for this col
                                    continue;
                                }

                                Object val = SerializerTypeInferer.getSerializer(field.getType()).fromBytes(col.getValue());
                                field.set(t, val);
                                columns.put((String) superCol.getName(), t);
                            }
                        }
                    }

                    superField.set(st, columns);
                }
            }
        } catch (IllegalAccessException e) {
            throw new ObjectNotSerializableException("Reflection Error ", e);
        }
    }

    /**
     * Gets the field for property name.
     *
     * @param <T>    the generic type
     * @param entity the entity
     * @param name   the name
     * @return the field for property name
     */
    public static <T> Field getFieldForPropertyName(T entity, String name) {
        try {
            return entity.getClass().getField(name);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Populate entity from cols.
     *
     * @param entity the entity
     * @param cols   the cols
     */
    public static void populateEntityFromCols(Object entity, List<HColumn<String, String>> cols) {

        for (HColumn<String, ?> col : cols) {
            Field f = getFieldForPropertyName(entity, col.getName());
            try {
                f.setAccessible(true);
                f.set(entity, col.getValue());
            } catch (IllegalAccessException e) {
                throw new ObjectNotSerializableException(e);
            }
        }
    }

    /**
     * Gets the all column names.
     *
     * @param entityClass the entity class
     * @return the all column names
     */
    public static String[] getAllColumnNames(Class<?> entityClass) {
        List<String> columnNames = new ArrayList<String>();
        Iterable<Field> fields = getFieldsUpTo(entityClass, null);
        //Field[] fields = entityClass.getDeclaredFields();

        for (Field field : fields) {

            field.setAccessible(true);
            String name = field.getName();
            columnNames.add(name);
        }

        return columnNames.toArray(new String[]{});
    }

    /**
     * Gets the column count.
     *
     * @param entityClass the entity class
     * @return the column count
     */
    public static int getColumnCount(Class<?> entityClass) {
        String[] columnNames = getAllColumnNames(entityClass);
        return columnNames.length;
    }
}

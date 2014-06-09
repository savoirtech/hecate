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

import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import com.savoirtech.hecate.cql3.annotations.Id;
import com.savoirtech.hecate.cql3.annotations.IdColumn;
import com.savoirtech.hecate.cql3.annotations.TableName;
import com.savoirtech.hecate.cql3.dao.abstracts.GenericCqlDao;
import com.savoirtech.hecate.cql3.dao.abstracts.GenericPojoGraphDao;
import com.savoirtech.hecate.cql3.exception.HecateException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectionUtils {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final static Logger LOGGER = LoggerFactory.getLogger(ReflectionUtils.class);
    public static final TypeVariable<Class<List>> LIST_ELEMENT_TYPE_VAR = List.class.getTypeParameters()[0];
    public static final TypeVariable<Class<Set>> SET_ELEMENT_TYPE_VAR = Set.class.getTypeParameters()[0];
    public static final TypeVariable<Class<Map>> MAP_KEY_TYPE_VAR = Map.class.getTypeParameters()[0];
    public static final TypeVariable<Class<Map>> MAP_VALUE_TYPE_VAR = Map.class.getTypeParameters()[1];
    private static final String IS_PREFIX = "is";
    private static final String GET_PREFIX = "get";
    private static final String SET_PREFIX = "set";

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static void collectFields(Class<?> type, List<Field> fields) {
        final Field[] declaredFields = type.getDeclaredFields();
        for (Field declaredField : declaredFields) {
            if (isPersistable(declaredField)) {
                fields.add(declaredField);
            }
        }
        if (type.getSuperclass() != null) {
            collectFields(type.getSuperclass(), fields);
        }
    }

    public static <K> K extractFieldValue(String fieldName, Field fieldType, Row row) {
        return null;
    }

    public static String[] fieldNames(Class mappingClazz) {
        List<String> fields = new ArrayList<>();
        for (Field field : getFieldsUpTo(mappingClazz, null)) {
            fields.add(field.getName());
        }
        return fields.toArray(new String[fields.size()]);
    }

    public static <T> Object[] fieldValues(T pojo) {
        List vals = new ArrayList();
        for (Field field : getFieldsUpTo(pojo.getClass(), null)) {
            try {
                field.setAccessible(true);

                Object value = field.get(pojo);

                vals.add(value);
            }
            catch (IllegalAccessException e) {
                LOGGER.error("Could not access field " + e);
            }
        }
        return vals.toArray(new Object[vals.size()]);
    }

    private static String getClassName(Object target) {
        return target == null ? "null" : target.getClass().getCanonicalName();
    }

    public static Field getFieldType(String id) {
        return null;
    }

    public static Object getFieldValue(Field field, Object target) {
        try {
            LOGGER.debug("Getting field {} value from object {} (type={})...", field.getName(), target, getClassName(target));
            return FieldUtils.readField(field, target, true);
        }
        catch (IllegalAccessException e) {
            throw new HecateException(String.format("Unable to read field %s value from object of type %s.", field.getName(), getClassName(target)), e);
        }
    }

    public static List<Field> getFields(Class<?> pojoType) {
        List<Field> fields = new LinkedList<>();
        collectFields(pojoType, fields);
        return fields;
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

    public static <K> String getIdName(Class clazz) {
        for (Field fied : getFieldsUpTo(clazz, null)) {
            if (fied.isAnnotationPresent(IdColumn.class) || fied.isAnnotationPresent(Id.class)) {
                return fied.getName();
            }
        }

        return null;
    }

    public static Class getIdType(Class instanceClazz) {
        for (Field fied : getFieldsUpTo(instanceClazz, null)) {
            if (fied.isAnnotationPresent(IdColumn.class) || fied.isAnnotationPresent(Id.class)) {
                return fied.getType();
            }
        }

        return null;
    }

    public static <P> P instantiate(Class<P> type) {
        try {
            return type.newInstance();
        }
        catch (InstantiationException e) {
            throw new HecateException(String.format("Unable to instantiate object of type %s.", type.getName()), e);
        }
        catch (IllegalAccessException e) {
            throw new HecateException(String.format("Default constructor not accessible for class %s.", type.getName()), e);
        }
    }

    public static boolean isPersistable(Field field) {
        final int mods = field.getModifiers();
        return !(Modifier.isFinal(mods) ||
                Modifier.isTransient(mods) ||
                Modifier.isStatic(mods));
    }

    public static Class<?> mapKeyType(Type type) {
        return findTypeVariable(type, Map.class, MAP_KEY_TYPE_VAR);
    }

    public static Object invoke(Object target, Method method, Object... params) {
        try {
            Validate.notNull(method).setAccessible(true);
            return method.invoke(target, params);
        }
        catch (IllegalAccessException e) {
            throw new HecateException(String.format("Unable to invoke method %s on object of type %s.", method.getName(), getClassName(target)), e);
        }
        catch (InvocationTargetException e) {
            throw new HecateException(String.format("Method %s threw exception when invoked on an object of type %s.", method.getName(), getClassName(target)));
        }
    }

    public static Class<?> mapValueType(Type type) {
        return findTypeVariable(type, Map.class, MAP_VALUE_TYPE_VAR);
    }

    private static <T> Class<?> findTypeVariable(Type type, Class<T> declaringClass, TypeVariable<Class<T>> target) {
        final Map<TypeVariable<?>, Type> arguments = TypeUtils.getTypeArguments(type, declaringClass);
        for (Map.Entry<TypeVariable<?>, Type> entry : arguments.entrySet()) {
            if (target.equals(entry.getKey())) {
                return TypeUtils.getRawType(entry.getValue(), type);
            }
        }
        return null;
    }

    public static Class<?> listElementType(Type type) {
        return findTypeVariable(type, List.class, LIST_ELEMENT_TYPE_VAR);
    }

    @SuppressWarnings("unchecked")
    static <T> T[] newArray(Class<T> type, int length) {
        return (T[]) Array.newInstance(type, length);
    }

    public static <T> Object[] pojofieldValues(T pojo) {
        List vals = new ArrayList();
        for (Field field : getFieldsUpTo(pojo.getClass(), null)) {
            try {
                field.setAccessible(true);

                //TODO - If this is another object, a collection or a Dictionary
                //we need to inspect this and generate the appropriate values.
                //We also need to iterate over each field and create new table inserts.

                Object value = field.get(pojo);
                vals.add(value);
            }
            catch (IllegalAccessException e) {
                LOGGER.error("Could not access field " + e);
            }
        }
        return vals.toArray(new Object[vals.size()]);
    }

    public static <T> void populate(T clz, Row row) {
        for (ColumnDefinitions.Definition cf : row.getColumnDefinitions()) {
            LOGGER.debug("Column " + cf.getType().asJavaClass());

            List<String> fields = Arrays.asList(fieldNames(clz.getClass()));
            try {
                for (String fname : fields) {
                    if (fname.equalsIgnoreCase(cf.getName())) {
                        Field field = clz.getClass().getDeclaredField(fname);
                        field.setAccessible(true);
                        field.set(clz, FieldMapper.getJavaObject(cf.getType().getName().name(), cf.getName(), row));
                    }
                }
            }
            catch (NoSuchFieldException e) {
                LOGGER.error("Trying to access a field that doesn't exist " + e);
            }
            catch (IllegalAccessException e) {
                LOGGER.error("Access problem " + e);
            }
        }
    }

    public static <T> void populateGraph(T clz, Row row, GenericCqlDao dao) throws HecateException {
        for (ColumnDefinitions.Definition cf : row.getColumnDefinitions()) {
            LOGGER.debug("Column " + cf.getType().asJavaClass());

            List<String> fields = Arrays.asList(fieldNames(clz.getClass()));
            try {
                boolean fieldAdded = false;
                for (String fname : fields) {
                    if (fname.equalsIgnoreCase(cf.getName())) {
                        Field field = clz.getClass().getDeclaredField(fname);
                        field.setAccessible(true);
                        LOGGER.debug("Adding in a " + field.toGenericString());
                        if (FieldMapper.getRawCassandraType(field) == null) {
                            LOGGER.debug("Looking up " + field.getGenericType() + " with key " + FieldMapper.getJavaObject(
                                    cf.getType().getName().name(), cf.getName(), row) + " from " + dao.getKeySpace() + "." + tableName(field));
                            Object id = FieldMapper.getJavaObject(cf.getType().getName().name(), cf.getName(), row);
                            if (id != null) {
                                Object o = ((GenericPojoGraphDao) dao).findChildRow(id, field.getType(), dao.getKeySpace(), tableName(field));
                                field.set(clz, o);
                            }
                            fieldAdded = true;
                        }

                        if ("list<blob>".equals(FieldMapper.getRawCassandraType(field))) {
                            LOGGER.debug("Looking up " + field.getGenericType() + " with key " + FieldMapper.getJavaObject(
                                    cf.getType().getName().name(), cf.getName(), row) + " from " + dao.getKeySpace() + "." + tableName(field));

                            Type type = field.getGenericType();
                            if (type instanceof ParameterizedType) {
                                ParameterizedType pt = (ParameterizedType) type;
                                Class entityClazz = typeToClass(pt.getActualTypeArguments()[0], field.getDeclaringClass().getClassLoader());
                                List idlist = (List) FieldMapper.getJavaObject(cf.getType().getName().name(), cf.getName(), row);
                                List entities = new ArrayList();
                                for (Object id : idlist) {
                                    if (id != null) {
                                        LOGGER.debug("Find list item " + id + " from " + dao.getKeySpace() + "." + tableName(field));
                                        Object o = ((GenericPojoGraphDao) dao).findChildRow(id, entityClazz, dao.getKeySpace(), tableName(field));
                                        LOGGER.debug("Found entity " + o);
                                        entities.add(o);
                                    }
                                }
                                field.set(clz, entities);
                                fieldAdded = true;
                            }
                        }

                        if ("set<blob>".equals(FieldMapper.getRawCassandraType(field))) {
                            LOGGER.debug("Looking up " + field.getGenericType() + " with key " + FieldMapper.getJavaObject(
                                    cf.getType().getName().name(), cf.getName(), row) + " from " + dao.getKeySpace() + "." + tableName(field));

                            Type type = field.getGenericType();
                            if (type instanceof ParameterizedType) {
                                ParameterizedType pt = (ParameterizedType) type;
                                Class entityClazz = typeToClass(pt.getActualTypeArguments()[0], field.getDeclaringClass().getClassLoader());
                                Set idlist = (Set) FieldMapper.getJavaObject(cf.getType().getName().name(), cf.getName(), row);
                                Set entities = new HashSet();
                                for (Object id : idlist) {
                                    if (id != null) {
                                        LOGGER.debug("Find set item " + id + " from " + dao.getKeySpace() + "." + tableName(field));
                                        Object o = ((GenericPojoGraphDao) dao).findChildRow(id, entityClazz, dao.getKeySpace(), tableName(field));
                                        LOGGER.debug("Found entity " + o);
                                        entities.add(o);
                                    }
                                }
                                field.set(clz, entities);
                                fieldAdded = true;
                            }
                        }

                        if (FieldMapper.getRawCassandraType(field) != null && FieldMapper.getRawCassandraType(field).toLowerCase().startsWith("map<")
                                && FieldMapper.getRawCassandraType(field).toLowerCase().contains(",blob>")) {
                            LOGGER.debug("Looking up " + field.getGenericType() + " with key " + FieldMapper.getJavaObject(
                                    cf.getType().getName().name(), cf.getName(), row) + " from " + dao.getKeySpace() + "." + tableName(field));

                            Type type = field.getGenericType();
                            if (type instanceof ParameterizedType) {
                                ParameterizedType pt = (ParameterizedType) type;
                                Class entityClazz = typeToClass(pt.getActualTypeArguments()[1], field.getDeclaringClass().getClassLoader());
                                Map idlist = (Map) FieldMapper.getJavaObject(cf.getType().getName().name(), cf.getName(), row);
                                Map entities = new HashMap();
                                for (Object ento : idlist.entrySet()) {
                                    Map.Entry ent = (Map.Entry) ento;
                                    Object id = ent.getKey();
                                    if (id != null) {
                                        LOGGER.debug("Find map item " + id + "=>" + ent.getValue() + " from " + dao.getKeySpace() + "." + tableName(
                                                field));
                                        Object o = ((GenericPojoGraphDao) dao).findChildRow(ent.getValue(), entityClazz, dao.getKeySpace(), tableName(
                                                field));
                                        LOGGER.debug("Found entity " + o);
                                        entities.put(id, o);
                                    }
                                }
                                field.set(clz, entities);
                                fieldAdded = true;
                            }
                        }

                        if (!fieldAdded) {
                            field.set(clz, FieldMapper.getJavaObject(cf.getType().getName().name(), cf.getName(), row));
                        }
                    }
                }
            }
            catch (NoSuchFieldException e) {
                LOGGER.error("Trying to access a field that doesn't exist " + e);
            }
            catch (IllegalAccessException e) {
                LOGGER.error("Access problem " + e);
            }
            catch (ClassNotFoundException e) {
                LOGGER.error("Class not found " + e);
            }
        }
    }

    public static Class<?> setElementType(Type type) {
        return findTypeVariable(type, Set.class, SET_ELEMENT_TYPE_VAR);

    }

    public static void setFieldValue(Field field, Object target, Object fieldValue) {
        try {
            LOGGER.debug("Setting field {} to value {} on object {} (type={})...", field.getName(), fieldValue, target, getClassName(target));
            FieldUtils.writeField(field, target, fieldValue, true);
        }
        catch (IllegalAccessException e) {
            throw new HecateException(String.format("Unable to write field %s value on object of type %s.", field.getName(), getClassName(target)), e);
        }
    }

    public static String tableName(Field field) {
        if (field.isAnnotationPresent(TableName.class)) {
            return field.getAnnotation(TableName.class).value();
        } else {
            return field.getName();
        }
    }

    public static Class typeToClass(Type type, ClassLoader cl) throws ClassNotFoundException {
        return cl.loadClass(type.toString().split(" ")[1]);
    }

    public static <T> Map<Class, Set<DataDescriptor>> valuesForClasses(Map<Class, Set<DataDescriptor>> values, String originalTableName,
                                                                       T pojo) throws HecateException {
        List vals = new ArrayList();

        DataDescriptor dataDescriptor = new DataDescriptor();
        dataDescriptor.tableName = originalTableName;

        for (Field field : getFieldsUpTo(pojo.getClass(), null)) {
            try {
                field.setAccessible(true);

                String csType = FieldMapper.getRawCassandraType(field);
                boolean fieldProcessed = false;
                if (csType == null) {
                    LOGGER.debug("Encountered an Object, we need to convert this object to a new insert value.");

                    Object fieldVal = field.get(pojo);
                    String id = ReflectionUtils.getIdName(field.getType());
                    dataDescriptor.tableName = tableName(field);

                    if (fieldVal != null) {
                        for (Field nestedF : getFieldsUpTo(fieldVal.getClass(), null)) {
                            nestedF.setAccessible(true);
                            if (id == null) {
                                throw new HecateException("Id field not found on object " + fieldVal);
                            }
                            if (id.equals(nestedF.getName())) {
                                vals.add(nestedF.get(fieldVal));
                                fieldProcessed = true;
                                valuesForClasses(values, dataDescriptor.getTableName(), fieldVal);
                            }
                        }
                    } else {
                        vals.add(null);
                        fieldProcessed = true;
                    }
                }

                if ("list<blob>".equalsIgnoreCase(csType)) {
                    LOGGER.debug("Encountered a List, checking generic type");
                    dataDescriptor.tableName = tableName(field);
                    List fieldVal = (List) field.get(pojo);
                    List rawList = new ArrayList();
                    LOGGER.debug("List " + fieldVal);
                    for (Object o : fieldVal) {
                        LOGGER.debug("Object class " + o.getClass());
                        String id = ReflectionUtils.getIdName(o.getClass());
                        for (Field nestedF : getFieldsUpTo(o.getClass(), null)) {
                            LOGGER.debug("Parsing list item " + nestedF);
                            nestedF.setAccessible(true);
                            if (id == null) {
                                throw new HecateException("Id field not found on list item " + fieldVal);
                            }
                            if (id.equals(nestedF.getName())) {
                                LOGGER.debug("Field to use " + field);
                                rawList.add(nestedF.get(o));
                                valuesForClasses(values, dataDescriptor.getTableName(), o);
                            }
                        }
                    }

                    vals.add(rawList);
                    fieldProcessed = true;
                }

                if ("set<blob>".equalsIgnoreCase(csType)) {
                    LOGGER.debug("Encountered a Set, checking generic type");
                    dataDescriptor.tableName = tableName(field);
                    Set fieldVal = (Set) field.get(pojo);

                    Set rawSet = new HashSet();
                    for (Object o : fieldVal) {
                        LOGGER.debug("Object class " + o.getClass());
                        String id = ReflectionUtils.getIdName(o.getClass());
                        for (Field nestedF : getFieldsUpTo(o.getClass(), null)) {
                            nestedF.setAccessible(true);
                            if (id == null) {
                                throw new HecateException("Id field not found on set item " + fieldVal);
                            }
                            if (id.equals(nestedF.getName())) {
                                rawSet.add(nestedF.get(o));
                                valuesForClasses(values, dataDescriptor.getTableName(), o);
                            }
                        }
                    }
                    vals.add(rawSet);
                    fieldProcessed = true;
                }

                if (csType != null && csType.toLowerCase().contains("map<") && csType.toLowerCase().contains(",blob>")) {
                    LOGGER.debug("Encountered a Map, checking generic type");
                    dataDescriptor.tableName = tableName(field);
                    Map fieldVal = (Map) field.get(pojo);

                    Map rawMap = new HashMap();
                    if (fieldVal != null) {
                        for (Object en : fieldVal.entrySet()) {
                            Map.Entry o = (Map.Entry) en;
                            LOGGER.debug("Object class " + o.getValue());
                            String id = ReflectionUtils.getIdName(o.getValue().getClass());
                            if (id == null) {
                                throw new HecateException("Id field not found on set item " + fieldVal);
                            }
                            for (Field nestedF : getFieldsUpTo(o.getValue().getClass(), null)) {
                                nestedF.setAccessible(true);

                                if (id.equals(nestedF.getName())) {
                                    rawMap.put(o.getKey(), nestedF.get(o.getValue()));
                                    valuesForClasses(values, dataDescriptor.getTableName(), o.getValue());
                                }
                            }
                        }
                    }
                    vals.add(rawMap);
                    fieldProcessed = true;
                }

                if (!fieldProcessed) {
                    Object value = field.get(pojo);
                    vals.add(value);
                }
            }
            catch (IllegalAccessException e) {
                LOGGER.error("Could not access field " + e);
            }
        }
        dataDescriptor.values = vals.toArray(new Object[vals.size()]);
        if (values.containsKey(pojo.getClass()) && values.get(pojo.getClass()) == null || !values.containsKey(pojo.getClass())) {
            values.put(pojo.getClass(), new HashSet<DataDescriptor>());
        }
        values.get(pojo.getClass()).add(dataDescriptor);

        for (Map.Entry<Class, Set<ReflectionUtils.DataDescriptor>> entry : values.entrySet()) {
            for (DataDescriptor descriptor : entry.getValue()) {
                LOGGER.debug(dataDescriptor.getTableName() + "=>" + Arrays.asList(descriptor.getValues()));
            }
        }
        return values;
    }

    private static String propertySuffix(PropertyDescriptor descriptor) {
        return StringUtils.capitalize(descriptor.getName());
    }

    private static String getReadMethodName(PropertyDescriptor descriptor) {
        final String prefix = Boolean.TYPE.equals(descriptor.getPropertyType()) ? IS_PREFIX : GET_PREFIX;
        return prefix + propertySuffix(descriptor);
    }

    private static String getWriteMethodName(PropertyDescriptor descriptor) {
        return SET_PREFIX + propertySuffix(descriptor);
    }

    private static Method findDeclaredMethod(Class<?> c, String name, Class<?>... parameterTypes) {
        try {
            return c.getDeclaredMethod(name, parameterTypes);
        }
        catch (NoSuchMethodException e) {
            if (c.getSuperclass() != null) {
                return findDeclaredMethod(c.getSuperclass(), name, parameterTypes);
            }
            return null;
        }
    }

    public static Method getReadMethod(Class<?> pojoType, PropertyDescriptor descriptor) {
        Method method = descriptor.getReadMethod();
        if (method == null) {
            Method candidate = findDeclaredMethod(pojoType, getReadMethodName(descriptor));
            if (candidate != null && descriptor.getPropertyType().equals(candidate.getReturnType())) {
                method = candidate;
            }
        }
        return method;
    }

    public static Method getWriteMethod(Class<?> pojoType, PropertyDescriptor descriptor) {
        Method method = descriptor.getWriteMethod();
        if (method == null) {
            Method candidate = findDeclaredMethod(pojoType, getWriteMethodName(descriptor), descriptor.getPropertyType());
            if (candidate != null && Void.TYPE.equals(candidate.getReturnType())) {
                method = candidate;
            }
        }
        return method;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    public static class DataDescriptor {
        String tableName;
        String id;

        Object[] values;

        public Object[] getValues() {
            return values;
        }

        public String getTableName() {
            return tableName;
        }

        @Override
        public String toString() {
            return "DataDescriptor{" +
                    "tableName='" + tableName + '\'' +
                    ", id='" + id + '\'' +
                    ", values=" + Arrays.toString(values) +
                    '}';
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setValues(Object[] values) {
            this.values = values;
        }
    }
}

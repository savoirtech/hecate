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

package com.savoirtech.hecate.cql3.dao.abstracts;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.exception.HecateException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GenericPojoGraphDao<K, T> extends GenericCqlDao<K, T> {
    public GenericPojoGraphDao(Session session, String keySpace, String tableName, Class keyClazz, Class mappingClazz) {
        super(session, keySpace, tableName, keyClazz, mappingClazz);
    }

    @Override
    public void delete(K key) {

        Object rootClass = find(key);

        if (rootClass != null) {
            Map<Class, Set<ReflectionUtils.DataDescriptor>> valueMap = new HashMap<>();

            try {
                ReflectionUtils.valuesForClasses(valueMap, null, rootClass);

                for (Map.Entry<Class, Set<ReflectionUtils.DataDescriptor>> entry : valueMap.entrySet()) {
                    Delete.Where delete = null;
                    if (entry.getKey().getName().equals(rootClass.getClass().getName())) {
                        logger.info("Working on the root class " + rootClass);
                        for (ReflectionUtils.DataDescriptor descriptor : entry.getValue()) {

                            delete = QueryBuilder.delete().all().from(keySpace, tableName).where(QueryBuilder.eq(ReflectionUtils.getIdName(
                                    entry.getKey()), key));
                            ResultSet res = session.execute(delete);

                            logger.debug("Result " + res);
                        }
                    } else {
                        for (ReflectionUtils.DataDescriptor descriptor : entry.getValue()) {
                            String id = ReflectionUtils.getIdName(entry.getKey());
                            String nestedtableName = descriptor.getTableName();
                            logger.debug("Delete builder " + descriptor + " from " + keySpace + "." + nestedtableName);
                            ResultSet res;
                            for (Object val : descriptor.getValues()) {

                                delete = QueryBuilder.delete().all().from(keySpace, nestedtableName).where(QueryBuilder.eq(id, val));

                                logger.debug("Delete " + delete);
                                res = session.execute(delete);
                                logger.debug("Result " + res);
                            }
                        }
                    }
                }
            }
            catch (HecateException e) {
                logger.error("Hecate problem " + e);
            }
        }
    }

    @Override
    public void save(T pojo) {
        //Find all Attached children and their respective classes.

        Map<Class, Set<ReflectionUtils.DataDescriptor>> valueMap = new HashMap<>();

        try {
            ReflectionUtils.valuesForClasses(valueMap, null, pojo);

            for (Map.Entry<Class, Set<ReflectionUtils.DataDescriptor>> entry : valueMap.entrySet()) {
                Insert insert = null;
                if (entry.getKey().getName().equals(pojo.getClass().getName())) {
                    logger.info("Working on the root class " + pojo);
                    for (ReflectionUtils.DataDescriptor descriptor : entry.getValue()) {
                        insert = QueryBuilder.insertInto(keySpace, tableName).values(ReflectionUtils.fieldNames(entry.getKey()),
                                descriptor.getValues());

                        logger.debug("Insert " + insert);
                        ResultSet res = session.execute(insert);
                        logger.debug("Result " + res);
                    }
                } else {
                    for (ReflectionUtils.DataDescriptor descriptor : entry.getValue()) {

                        String nestedtableName = descriptor.getTableName();
                        logger.debug("Insert builder " + descriptor + " into " + keySpace + "." + nestedtableName);
                        insert = QueryBuilder.insertInto(keySpace, nestedtableName).values(ReflectionUtils.fieldNames(entry.getKey()),
                                descriptor.getValues());
                        ResultSet res = session.execute(insert);
                        logger.debug("Result " + res);
                    }
                }
            }
        }
        catch (HecateException e) {
            logger.error("Hecate problem " + e);
        }
    }

    public T findChildRow(K key, Class mapping, String keySpace, String tableName) {
        Select.Where select = QueryBuilder.select(ReflectionUtils.fieldNames(mapping)).from(keySpace, tableName).where(QueryBuilder.eq(
                ReflectionUtils.getIdName(mapping), key));
        logger.debug("Find " + select);
        ResultSet res = session.execute(select);
        logger.debug("Found : " + res);
        if (res != null) {
            while (res.iterator().hasNext()) {
                Row row = res.iterator().next();
                try {
                    T clz = (T) mapping.newInstance();
                    ReflectionUtils.populateGraph(clz, row, this);
                    return clz;
                }
                catch (InstantiationException e) {
                    logger.error("Could not create class " + mapping + " " + e);
                }
                catch (IllegalAccessException e) {
                    logger.error("Could not access class " + mapping + " " + e);
                }
                catch (HecateException e) {
                    logger.error("Internal Hecate problem " + e);
                }
            }
        }

        return null;
    }

    @Override
    public T find(K key) {
        Select.Where select = QueryBuilder.select(ReflectionUtils.fieldNames(mappingClazz)).from(keySpace, tableName).where(QueryBuilder.eq(
                ReflectionUtils.getIdName(mappingClazz), key));
        logger.debug("Find " + select);
        ResultSet res = session.execute(select);
        if (res != null) {
            while (res.iterator().hasNext()) {
                Row row = res.iterator().next();
                try {
                    T clz = (T) mappingClazz.newInstance();
                    //Generate subSelects for the following - Object, Collection, Dictionary
                    ReflectionUtils.populateGraph(clz, row, this);
                    return clz;
                }
                catch (InstantiationException e) {
                    logger.error("Could not create class " + mappingClazz + " " + e);
                }
                catch (IllegalAccessException e) {
                    logger.error("Could not access class " + mappingClazz + " " + e);
                }
                catch (HecateException e) {
                    logger.error("Internal Hecate problem " + e);  //TODO
                }
            }
        }
        return null;
    }

    @Override
    public Set<T> findItems(List<K> keys) {

        Set<T> items = new HashSet<>();
        Select.Where select = QueryBuilder.select(ReflectionUtils.fieldNames(mappingClazz)).from(keySpace, tableName).where(QueryBuilder.in(
                ReflectionUtils.getIdName(mappingClazz), keys.toArray()));

        logger.debug("Find " + select);
        ResultSet res = session.execute(select);
        if (res != null) {

            while (res.iterator().hasNext()) {
                Row row = res.iterator().next();
                try {
                    T clz = (T) mappingClazz.newInstance();
                    ReflectionUtils.populate(clz, row);
                    items.add(clz);
                }
                catch (InstantiationException e) {
                    logger.error("Could not create class " + mappingClazz + " " + e);
                }
                catch (IllegalAccessException e) {
                    logger.error("Could not access class " + mappingClazz + " " + e);
                }
            }
        }

        return items;
    }
}

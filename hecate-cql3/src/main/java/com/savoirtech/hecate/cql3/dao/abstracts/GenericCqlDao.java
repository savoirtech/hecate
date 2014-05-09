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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.dao.GenericTableDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericCqlDao<K, T> implements GenericTableDao<K, T> {

    private Session session;
    private String tableName;
    private String keySpace;
    private Class mappingClazz;
    private Class keyClazz;

    private Logger logger = LoggerFactory.getLogger(GenericCqlDao.class);

    public GenericCqlDao(Session session, String keySpace, String tableName, Class keyClazz, Class mappingClazz) {
        this.session = session;
        this.tableName = tableName;
        this.keySpace = keySpace;
        this.keyClazz = keyClazz;
        this.mappingClazz = mappingClazz;
    }

    @Override
    public void delete(K key) {
        Delete.Where query = QueryBuilder.delete().all().from(keySpace, tableName).where(QueryBuilder.eq(ReflectionUtils.getIdName(mappingClazz),
            key));
        session.execute(query);
    }

    @Override
    public Set<K> getKeys() {
        Set<K> keys = new HashSet<>();
        Select selection = QueryBuilder.select().column(ReflectionUtils.getIdName(mappingClazz)).from(keySpace, tableName);
        ResultSet res = session.execute(selection);
        for (Row row : res.all()) {
            keys.add(ReflectionUtils.<K>extractFieldValue(ReflectionUtils.getIdName(mappingClazz), ReflectionUtils.getFieldType(
                ReflectionUtils.getIdName(mappingClazz)), row));
        }
        return keys;
    }

    @Override
    public boolean containsKey(K key) {
        Select.Where selection = QueryBuilder.select().column(ReflectionUtils.getIdName(mappingClazz)).from(keySpace, tableName).where(
            QueryBuilder.eq(ReflectionUtils.getIdName(mappingClazz), key));
        ResultSet res = session.execute(selection);
        return ((res.all().size() > 0));
    }

    @Override
    public void save(T pojo) {
        Insert insert = QueryBuilder.insertInto(keySpace, tableName).values(ReflectionUtils.fieldNames(mappingClazz), ReflectionUtils.fieldValues(
            pojo));
        logger.debug("Save " + insert);
        ResultSet res = session.execute(insert);
        logger.debug("Result " + res);
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
                    ReflectionUtils.populate(clz, row);
                    return clz;
                } catch (InstantiationException e) {
                    logger.error("Could not create class " + mappingClazz + " " + e);
                } catch (IllegalAccessException e) {
                    logger.error("Could not access class " + mappingClazz + " " + e);
                }
            }
        }

        return null;
    }

    @Override
    public Set<T> findItems(List<K> keys, String rangeFrom, String rangeTo) {
        return null;  //TODO
    }
}

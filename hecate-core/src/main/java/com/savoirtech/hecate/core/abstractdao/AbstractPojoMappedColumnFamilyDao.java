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

package com.savoirtech.hecate.core.abstractdao;

import com.savoirtech.hecate.core.config.CassandraKeyspaceConfigurator;
import com.savoirtech.hecate.core.config.HectorHelper;
import com.savoirtech.hecate.core.config.HectorManager;
import com.savoirtech.hecate.core.config.ObjectNotSerializableException;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractPojoMappedColumnFamilyDao<K, T> {

    /**
     * The key space.
     */
    protected final Keyspace keySpace;
    /**
     * The column family name.
     */
    protected final String columnFamilyName;
    /**
     * The persistent class.
     */
    protected final Class<T> persistentClass;
    /**
     * The key type class.
     */
    final Class<K> keyTypeClass;
    /**
     * The all column names.
     */
    private String[] allColumnNames;

    /**
     * Instantiates a new abstract column family dao.
     */
    public AbstractPojoMappedColumnFamilyDao(final String clusterName, final CassandraKeyspaceConfigurator keyspaceConfigurator,
                                             final Class<K> keyTypeClass, final Class<T> persistentClass, final String columnFamilyName,
                                             String comparatorAlias) {
        this.keySpace = new HectorManager().getKeyspace(clusterName, keyspaceConfigurator, columnFamilyName, false, comparatorAlias);
        this.keyTypeClass = keyTypeClass;
        this.persistentClass = persistentClass;
        this.columnFamilyName = columnFamilyName;
        this.allColumnNames = HectorHelper.getAllColumnNames(persistentClass);
    }

    /**
     * Save.
     *
     * @param key   the key
     * @param model the model
     */
    public void save(K key, T model) {

        Mutator<Object> mutator = HFactory.createMutator(keySpace, SerializerTypeInferer.getSerializer(keyTypeClass));

        for (HColumn<?, ?> column : HectorHelper.getColumnsNoAnnotations(model)) {
            mutator.addInsertion(key, columnFamilyName, column);
        }

        mutator.execute();
    }

    /**
     * Find.
     *
     * @param key the key
     * @return the t
     */
    public T find(K key) {
        SliceQuery<Object, String, byte[]> query = HFactory.createSliceQuery(keySpace, SerializerTypeInferer.getSerializer(keyTypeClass),
                StringSerializer.get(), BytesArraySerializer.get());

        QueryResult<ColumnSlice<String, byte[]>> result = query.setColumnFamily(columnFamilyName).setKey(key).setRange("", "", false,
                Integer.MAX_VALUE).execute();

        try {
            if (result.get().getColumns().isEmpty()) {
                return null;
            }
        }
        catch (Exception e) {
            return null;
        }

        try {
            T t = persistentClass.newInstance();
            HectorHelper.populateEntityAnnotated(t, result);
            return t;
        }
        catch (Exception e) {
            throw new ObjectNotSerializableException("Error creating persistent class", e);
        }
    }

    /**
     * Find.
     *
     * @param key the key
     * @return the t
     */
    public T findAllColumns(K key) {
        SliceQuery<Object, String, byte[]> query = HFactory.createSliceQuery(keySpace, SerializerTypeInferer.getSerializer(keyTypeClass),
                StringSerializer.get(), BytesArraySerializer.get());

        QueryResult<ColumnSlice<String, byte[]>> result = query.setColumnFamily(columnFamilyName).setKey(key).execute();

        try {
            if (result.get().getColumns().isEmpty()) {
                return null;
            }
        }
        catch (Exception e) {
            return null;
        }

        try {
            T t = persistentClass.newInstance();
            HectorHelper.populateEntityAnnotated(t, result);
            return t;
        }
        catch (Exception e) {
            throw new ObjectNotSerializableException("Error creating persistent class", e);
        }
    }

    /**
     * Delete.
     */
    public void delete(K key) {
        Mutator<Object> mutator = HFactory.createMutator(keySpace, SerializerTypeInferer.getSerializer(keyTypeClass));
        mutator.delete(key, columnFamilyName, null, SerializerTypeInferer.getSerializer(keyTypeClass));
    }

    /**
     * Gets the keys.
     *
     * @return the keys
     */
    public Set<K> getKeys() {
        int rows = 0;
        int pagination = 50;
        Set<K> rowKeys = new HashSet<K>();

        Row<Object, String, byte[]> lastRow = null;

        do {
            RangeSlicesQuery<Object, String, byte[]> rangeSlicesQuery = HFactory.createRangeSlicesQuery(keySpace, SerializerTypeInferer.getSerializer(
                    keyTypeClass), StringSerializer.get(), BytesArraySerializer.get());
            rangeSlicesQuery.setColumnFamily(columnFamilyName);
            if (lastRow != null) {
                rangeSlicesQuery.setKeys(lastRow.getKey(), "");
            } else {
                rangeSlicesQuery.setKeys(null, null);
            }
            rangeSlicesQuery.setReturnKeysOnly();
            rangeSlicesQuery.setRange("", "", false, keyTypeClass.getDeclaredFields().length);
            rangeSlicesQuery.setRowCount(pagination);
            QueryResult<OrderedRows<Object, String, byte[]>> result = rangeSlicesQuery.execute();
            OrderedRows<Object, String, byte[]> orderedRows = result.get();
            rows = orderedRows.getCount();

            for (Row<Object, String, byte[]> row : orderedRows) {
                if (!row.getColumnSlice().getColumns().isEmpty()) {
                    rowKeys.add((K) row.getKey());
                }
            }

            lastRow = orderedRows.peekLast();
        }
        while (rows == pagination);

        return rowKeys;
    }

    /**
     * Find items.
     *
     * @param keys      the keys
     * @param rangeFrom the range from
     * @param rangeTo   the range to
     * @return the sets the
     */
    public Set<T> findItems(final List<K> keys, final String rangeFrom, final String rangeTo) {

        Set<T> items = new HashSet<T>();

        MultigetSliceQuery<Object, String, byte[]> multigetSliceQuery = HFactory.createMultigetSliceQuery(keySpace,
                SerializerTypeInferer.getSerializer(keyTypeClass), StringSerializer.get(), BytesArraySerializer.get());

        multigetSliceQuery.setColumnFamily(columnFamilyName);
        multigetSliceQuery.setKeys(keys.toArray());
        multigetSliceQuery.setRange(rangeFrom, rangeTo, false, Integer.MAX_VALUE);

        QueryResult<Rows<Object, String, byte[]>> result = multigetSliceQuery.execute();

        for (Row<Object, String, byte[]> row : result.get()) {
            if (!row.getColumnSlice().getColumns().isEmpty()) {
                items.add((T) row.getColumnSlice());
            }
        }

        return items;
    }

    /**
     * Contains.
     *
     * @param key the key
     * @return true, if successful
     */
    public boolean containsKey(K key) {
        RangeSlicesQuery<Object, String, byte[]> rangeSlicesQuery = HFactory.createRangeSlicesQuery(keySpace, SerializerTypeInferer.getSerializer(
                keyTypeClass), StringSerializer.get(), BytesArraySerializer.get());
        rangeSlicesQuery.setColumnFamily(columnFamilyName);
        rangeSlicesQuery.setKeys(key, key);
        rangeSlicesQuery.setReturnKeysOnly();
        rangeSlicesQuery.setRange("", "", false, 1);
        rangeSlicesQuery.setRowCount(1);
        QueryResult<OrderedRows<Object, String, byte[]>> result = rangeSlicesQuery.execute();
        OrderedRows<Object, String, byte[]> orderedRows = result.get();

        return (!orderedRows.getList().isEmpty() && !orderedRows.getByKey(key).getColumnSlice().getColumns().isEmpty());
    }
}
